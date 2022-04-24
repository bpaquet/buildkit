package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	sess "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/containerd/containerd/content"
	"github.com/moby/buildkit/cache/remotecache"
	v1 "github.com/moby/buildkit/cache/remotecache/v1"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/worker"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

const (
	attrBucket          = "bucket"
	attrRegion          = "region"
	attrRole            = "role"
	attrSession         = "session"
	attrToken           = "token"
	attrPrefix          = "prefix"
	attrManifestsPrefix = "manifests_prefix"
	attrBlobsPrefix     = "blobs_prefix"
	attrName            = "name"
	attrTouchRefresh    = "touch_refresh"
)

type Config struct {
	Bucket          string
	Region          string
	Role            string
	Session         string
	Token           string
	Prefix          string
	ManifestsPrefix string
	BlobsPrefix     string
	Name            string
	TouchRefresh    time.Duration
}

func getConfig(attrs map[string]string) (*Config, error) {
	bucket, ok := attrs[attrBucket]
	if !ok {
		bucket, ok = os.LookupEnv("AWS_BUCKET")
		if !ok {
			errors.Errorf("bucket ($AWS_BUCKET) not set for s3 cache")
		}
	}

	region, ok := attrs[attrRegion]
	if !ok {
		region, ok = os.LookupEnv("AWS_REGION")
		if !ok {
			errors.Errorf("region ($AWS_REGION) not set for s3 cache")
		}
	}

	role, ok := attrs[attrRole]
	if !ok {
		role = os.Getenv("AWS_ROLE_ARN")
	}

	session, ok := attrs[attrSession]
	if !ok {
		session = os.Getenv("AWS_ROLE_SESSION_NAME")
	}

	token, ok := attrs[attrToken]
	if !ok {
		token = os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	}

	prefix, ok := attrs[attrPrefix]
	if !ok {
		prefix = ""
	}

	manifestsPrefix, ok := attrs[attrManifestsPrefix]
	if !ok {
		manifestsPrefix = "manifests/"
	}

	blobsPrefix, ok := attrs[attrBlobsPrefix]
	if !ok {
		blobsPrefix = "blobs/"
	}

	name, ok := attrs[attrName]
	if !ok {
		name = "buildkit"
	}

	touchRefresh, _ := time.ParseDuration("24h")

	touchRefreshStr, ok := attrs[attrTouchRefresh]
	if ok {
		touchRefreshFromUser, err := time.ParseDuration(touchRefreshStr)
		if err == nil {
			touchRefresh = touchRefreshFromUser
		}
	}

	return &Config{
		Bucket:          bucket,
		Region:          region,
		Role:            role,
		Session:         session,
		Token:           token,
		Prefix:          prefix,
		ManifestsPrefix: manifestsPrefix,
		BlobsPrefix:     blobsPrefix,
		Name:            name,
		TouchRefresh:    touchRefresh,
	}, nil
}

// ResolveCacheExporterFunc for s3 cache exporter.
func ResolveCacheExporterFunc() remotecache.ResolveCacheExporterFunc {
	return func(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Exporter, error) {
		config, err := getConfig(attrs)
		if err != nil {
			return nil, err
		}

		return NewExporter(config)
	}
}

type exporter struct {
	solver.CacheExporterTarget
	chains *v1.CacheChains
	cache  *cache
	config *Config
}

func NewExporter(config *Config) (remotecache.Exporter, error) {
	cc := v1.NewCacheChains()
	cache, err := newCache(config)
	if err != nil {
		return nil, err
	}
	return &exporter{CacheExporterTarget: cc, chains: cc, cache: cache, config: config}, nil
}

func (e *exporter) Config() remotecache.Config {
	return remotecache.Config{}
}

func (e *exporter) Finalize(ctx context.Context) (map[string]string, error) {
	cacheConfig, descs, err := e.chains.Marshal(ctx)
	if err != nil {
		return nil, err
	}

	for i, l := range cacheConfig.Layers {
		dgstPair, ok := descs[l.Blob]
		if !ok {
			return nil, errors.Errorf("missing blob %s", l.Blob)
		}
		if dgstPair.Descriptor.Annotations == nil {
			return nil, errors.Errorf("invalid descriptor without annotations")
		}
		var diffID digest.Digest
		v, ok := dgstPair.Descriptor.Annotations["containerd.io/uncompressed"]
		if !ok {
			return nil, errors.Errorf("invalid descriptor without uncompressed annotation")
		}
		dgst, err := digest.Parse(v)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse uncompressed annotation")
		}
		diffID = dgst

		key := blobKey(e.config, dgstPair.Descriptor.Digest)
		exists, err := e.cache.exists(key)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to check file presence in cache")
		}
		if exists != nil {
			if time.Since(*exists) > e.config.TouchRefresh {
				err = e.cache.touch(key)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to touch file")
				}
			}
		} else {
			layerDone := oneOffProgress(ctx, fmt.Sprintf("writing layer %s", l.Blob))
			bytes, err := content.ReadBlob(ctx, dgstPair.Provider, dgstPair.Descriptor)
			if err != nil {
				return nil, layerDone(err)
			}
			if err := e.cache.saveMutable(key, string(bytes)); err != nil {
				return nil, layerDone(errors.Wrap(err, "error writing layer blob"))
			}

			layerDone(nil)
		}

		la := &v1.LayerAnnotations{
			DiffID:    diffID,
			Size:      dgstPair.Descriptor.Size,
			MediaType: dgstPair.Descriptor.MediaType,
		}
		if v, ok := dgstPair.Descriptor.Annotations["buildkit/createdat"]; ok {
			var t time.Time
			if err := (&t).UnmarshalText([]byte(v)); err != nil {
				return nil, err
			}
			la.CreatedAt = t.UTC()
		}
		cacheConfig.Layers[i].Annotations = la
	}

	dt, err := json.Marshal(cacheConfig)
	if err != nil {
		return nil, err
	}

	if err := e.cache.saveMutable(manifestKey(e.config), string(dt)); err != nil {
		return nil, errors.Wrap(err, "error writing manifest")
	}

	return nil, nil
}

// ResolveCacheImporterFunc for s3 cache importer.
func ResolveCacheImporterFunc() remotecache.ResolveCacheImporterFunc {
	return func(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Importer, ocispecs.Descriptor, error) {
		config, err := getConfig(attrs)
		if err != nil {
			return nil, ocispecs.Descriptor{}, err
		}
		cache, err := newCache(config)
		if err != nil {
			return nil, ocispecs.Descriptor{}, err
		}
		return &importer{cache, config}, ocispecs.Descriptor{}, nil
	}
}

type importer struct {
	cache  *cache
	config *Config
}

func (i *importer) makeDescriptorProviderPair(l v1.CacheLayer) (*v1.DescriptorProviderPair, error) {
	if l.Annotations == nil {
		return nil, errors.Errorf("cache layer with missing annotations")
	}
	annotations := map[string]string{}
	if l.Annotations.DiffID == "" {
		return nil, errors.Errorf("cache layer with missing diffid")
	}
	annotations["containerd.io/uncompressed"] = l.Annotations.DiffID.String()
	if !l.Annotations.CreatedAt.IsZero() {
		txt, err := l.Annotations.CreatedAt.MarshalText()
		if err != nil {
			return nil, err
		}
		annotations["buildkit/createdat"] = string(txt)
	}
	desc := ocispecs.Descriptor{
		MediaType:   l.Annotations.MediaType,
		Digest:      l.Blob,
		Size:        l.Annotations.Size,
		Annotations: annotations,
	}
	return &v1.DescriptorProviderPair{
		Descriptor: desc,
		Provider:   &s3Provider{i.cache, i.config},
	}, nil
}

func (i *importer) load() (*v1.CacheChains, error) {
	b, err := i.cache.get(manifestKey(i.config))
	if err != nil {
		return nil, err
	}
	if b == "" {
		return v1.NewCacheChains(), nil
	}

	var config v1.CacheConfig
	if err := json.Unmarshal([]byte(b), &config); err != nil {
		return nil, errors.WithStack(err)
	}

	allLayers := v1.DescriptorProvider{}

	for _, l := range config.Layers {
		dpp, err := i.makeDescriptorProviderPair(l)
		if err != nil {
			return nil, err
		}
		allLayers[l.Blob] = *dpp
	}

	cc := v1.NewCacheChains()
	if err := v1.ParseConfig(config, allLayers, cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func (i *importer) Resolve(ctx context.Context, _ ocispecs.Descriptor, id string, w worker.Worker) (solver.CacheManager, error) {
	cc, err := i.load()
	if err != nil {
		return nil, err
	}

	keysStorage, resultStorage, err := v1.NewCacheKeyStorage(cc, w)
	if err != nil {
		return nil, err
	}

	return solver.NewCacheManager(ctx, id, keysStorage, resultStorage), nil
}

type s3Provider struct {
	cache  *cache
	config *Config
}

func (p *s3Provider) ReaderAt(ctx context.Context, desc ocispecs.Descriptor) (content.ReaderAt, error) {
	b, err := p.cache.get(blobKey(p.config, desc.Digest))
	if err != nil {
		return nil, err
	}
	if b == "" {
		return nil, errors.Errorf("blob not found")
	}
	return &readerAt{strings.NewReader(b), desc.Size}, nil
}

type readerAt struct {
	*strings.Reader
	size int64
}

func (r readerAt) Size() int64 {
	return r.size
}

func (readerAt) Close() error {
	return nil
}

func oneOffProgress(ctx context.Context, id string) func(err error) error {
	pw, _, _ := progress.NewFromContext(ctx)
	now := time.Now()
	st := progress.Status{
		Started: &now,
	}
	pw.Write(id, st)
	return func(err error) error {
		now := time.Now()
		st.Completed = &now
		pw.Write(id, st)
		pw.Close()
		return err
	}
}

func manifestKey(config *Config) string {
	return config.Prefix + config.ManifestsPrefix + config.Name
}

func blobKey(config *Config, dgst digest.Digest) string {
	return config.Prefix + config.BlobsPrefix + dgst.String()
}

type cache struct {
	config *Config

	client   *s3.S3
	uploader *s3manager.Uploader
}

func newCache(config *Config) (*cache, error) {
	client, err := newClient(config.Region, config.Role, config.Session, config.Token)
	if err != nil {
		return nil, err
	}
	return &cache{
		config: config,

		client:   client,
		uploader: s3manager.NewUploaderWithClient(client),
	}, nil
}

// Note: We dont use the s3manager.Downloader here, performance are lower.
func (c *cache) get(key string) (string, error) {
	fmt.Printf("Start downloading s3://%s/%s\n", c.config.Bucket, key)
	start := time.Now()
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(key),
	}

	output, err := c.client.GetObject(input)
	if err != nil {
		if isNotFound(err) {
			return "", nil
		}
		return "", err
	}
	defer output.Body.Close()
	bytes, err := ioutil.ReadAll(output.Body)
	if err != nil {
		return "", err
	}

	elapsed := time.Since(start)
	fmt.Printf("Download s3://%s/%s: %s (%d bytes)\n", c.config.Bucket, key, elapsed, len(bytes))
	return string(bytes), nil
}

func (c *cache) saveMutable(key, value string) error {
	fmt.Printf("Start uploading s3://%s/%s\n", c.config.Bucket, key)

	start := time.Now()
	input := &s3manager.UploadInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(value),
	}
	_, err := c.uploader.Upload(input)
	elapsed := time.Since(start)
	fmt.Printf("Uploading s3://%s/%s: %s (%d bytes)\n", c.config.Bucket, key, elapsed, len(value))
	return err
}

func (c *cache) exists(key string) (*time.Time, error) {
	fmt.Printf("Testing %s %s\n", c.config.Bucket, key)

	input := &s3.HeadObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(key),
	}

	head, err := c.client.HeadObject(input)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return head.LastModified, nil
}

func (c *cache) touch(key string) error {
	fmt.Printf("Touch s3://%s/%s\n", c.config.Bucket, key)

	start := time.Now()

	copy := &s3.CopyObjectInput{
		Bucket:     aws.String(c.config.Bucket),
		CopySource: aws.String(fmt.Sprintf("%s/%s", c.config.Bucket, key)),
		Key:        aws.String(key),
	}

	_, err := c.client.CopyObject(copy)

	elapsed := time.Since(start)
	fmt.Printf("Uploading s3://%s/%s: %s\n", c.config.Bucket, key, elapsed)
	return err
}

func newClient(region, role, sessionName, token string) (*s3.S3, error) {
	s, err := sess.NewSession(&aws.Config{Region: &region})
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %v", err)
	}

	if role == "" {
		return s3.New(s), nil
	}

	var creds *credentials.Credentials
	if token != "" {
		creds = stscreds.NewWebIdentityCredentials(s, role, sessionName, token)
	} else {
		creds = stscreds.NewCredentials(s, role)
	}

	return s3.New(s, &aws.Config{Credentials: creds}), nil
}

func isNotFound(err error) bool {
	awsErr, ok := err.(awserr.Error)
	if ok && awsErr.Code() == s3.ErrCodeNoSuchKey || awsErr.Code() == "NotFound" {
		return true
	}
	return false
}

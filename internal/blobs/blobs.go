// Package blobs is the title storage's blob tier: object bodies too large to keep inline live in
// one S3-compatible bucket (MinIO on the box), and consoles move the bytes themselves with
// short-lived presigned requests. The database row stays the index; this package never sees a
// title, an owner or a visibility.
//
// Two clients share the credentials: the internal one dials Endpoint (stat, digest, delete,
// bucket creation); the public one only signs, for PublicURL, because a SigV4 signature binds the
// host the console will connect to.
package blobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrMissing is a key with no object behind it (not uploaded yet, or already deleted).
var ErrMissing = errors.New("title-storage: blob not uploaded")

type Config struct {
	Endpoint  string // what this process dials, e.g. http://openpak-minio:9000
	PublicURL string // what consoles are told, e.g. http://title-storage.openpak.org (default: Endpoint)
	Bucket    string
	Prefix    string // key prefix inside the bucket, e.g. "objects/"
	AccessKey string
	SecretKey string
	Region    string // default us-east-1 (MinIO's)
}

type Store struct {
	in, pub *minio.Client
	bucket  string
	prefix  string
}

func dial(endpoint string, cfg Config) (*minio.Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("title-storage: S3 endpoint %q: want http(s)://host[:port]", endpoint)
	}
	// Region set and path-style lookup: signing never asks the server where the bucket lives, so
	// the public client works without a route to PublicURL.
	return minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       u.Scheme == "https",
		Region:       cfg.Region,
		BucketLookup: minio.BucketLookupPath,
	})
}

// New opens the bucket. Nothing is dialled until the first call.
func New(cfg Config) (*Store, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, errors.New("title-storage: S3 endpoint, bucket, access key and secret key are all required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.PublicURL == "" {
		cfg.PublicURL = cfg.Endpoint
	}
	in, err := dial(cfg.Endpoint, cfg)
	if err != nil {
		return nil, err
	}
	pub, err := dial(cfg.PublicURL, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{in: in, pub: pub, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

func (s *Store) obj(key string) string { return s.prefix + key }

// EnsureBucket creates the bucket when it is missing.
func (s *Store) EnsureBucket(ctx context.Context) error {
	ok, err := s.in.BucketExists(ctx, s.bucket)
	if err != nil || ok {
		return err
	}
	err = s.in.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
	if c := minio.ToErrorResponse(err).Code; c == "BucketAlreadyOwnedByYou" || c == "BucketAlreadyExists" {
		return nil
	}
	return err
}

// PresignPut is a URL for one plain PUT of the body.
func (s *Store) PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error) {
	u, err := s.pub.PresignedPutObject(ctx, s.bucket, s.obj(key), ttl)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

// PresignPost is a browser-style POST form (what NEX DataStore consoles send): the URL, and the
// fields to send before the file. The policy pins the key and the exact size.
func (s *Store) PresignPost(ctx context.Context, key string, size int64, ttl time.Duration) (string, map[string]string, error) {
	p := minio.NewPostPolicy()
	if err := p.SetBucket(s.bucket); err != nil {
		return "", nil, err
	}
	if err := p.SetKey(s.obj(key)); err != nil {
		return "", nil, err
	}
	if err := p.SetExpires(time.Now().UTC().Add(ttl)); err != nil {
		return "", nil, err
	}
	if err := p.SetContentLengthRange(size, size); err != nil {
		return "", nil, err
	}
	u, fields, err := s.pub.PresignedPostPolicy(ctx, p)
	if err != nil {
		return "", nil, err
	}
	return u.String(), fields, nil
}

// PresignGet is a URL for one GET of the body.
func (s *Store) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	u, err := s.pub.PresignedGetObject(ctx, s.bucket, s.obj(key), ttl, url.Values{})
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func missing(err error) bool {
	c := minio.ToErrorResponse(err).Code
	return c == "NoSuchKey" || c == "NotFound" || c == "NoSuchObject"
}

// Size is the stored object's size; ErrMissing when there is none.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	st, err := s.in.StatObject(ctx, s.bucket, s.obj(key), minio.StatObjectOptions{})
	if missing(err) {
		return 0, ErrMissing
	}
	if err != nil {
		return 0, err
	}
	return st.Size, nil
}

// SHA256 reads the object and returns its digest (lowercase hex).
func (s *Store) SHA256(ctx context.Context, key string) (string, error) {
	o, err := s.in.GetObject(ctx, s.bucket, s.obj(key), minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}
	defer o.Close()
	h := sha256.New()
	if _, err := io.Copy(h, o); err != nil {
		if missing(err) {
			return "", ErrMissing
		}
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Delete removes the object; one that is already gone is not an error (repeats are safe).
func (s *Store) Delete(ctx context.Context, key string) error {
	err := s.in.RemoveObject(ctx, s.bucket, s.obj(key), minio.RemoveObjectOptions{})
	if err != nil && !missing(err) {
		return err
	}
	return nil
}

// ValidDigest reports whether d is empty or a SHA-256 in hex.
func ValidDigest(d string) bool {
	if d == "" {
		return true
	}
	b, err := hex.DecodeString(d)
	return err == nil && len(b) == sha256.Size && d == strings.ToLower(d)
}

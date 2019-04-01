/*
s3bin downloads or uploads binary files from/to a AWS S3 bucket.

With the -put flag, s3bin uploads the file to the S3 bucket, and creates a
file with the same name plus the .sha1 extension. This file will contain the
SHA1 hash of the uploaded binary.

With the -get flag, s3bin takes the sha1 file created by -put and downloads
the corresponding file from S3 iff the corresponding local file dos not exist
or its contents do not match the provided hash.
*/
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

const version = 1

type Header struct {
	Version int `json:"version"`
}

type s3Bin struct {
	s3Bucket string
	s3Cli    *s3.S3
}

func newS3Bin(region, bucket string) (*s3Bin, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create AWS session")
	}

	s3Cli := s3.New(sess, &aws.Config{
		Region: aws.String(region),
	})

	return &s3Bin{
		s3Bucket: bucket,
		s3Cli:    s3Cli,
	}, nil
}

func (b *s3Bin) Put(path string) error {
	hash, err := calcSha1(path)
	if err != nil {
		return err
	}

	f, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "failed to open file")
	}
	defer f.Close()

	fstat, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "failed to read file attributes")
	}

	header := &Header{
		Version: version,
	}

	headerBytes, err := json.Marshal(header)
	if err != nil {
		return errors.Wrap(err, "json.Marshal(header)")
	}

	gzippedBuf := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(gzippedBuf)
	tarWriter := tar.NewWriter(gzipWriter)

	err = tarWriter.WriteHeader(&tar.Header{
		Name: "header",
		Mode: 0600,
		Size: int64(len(headerBytes)),
	})
	if err != nil {
		return errors.Wrap(err, "tarWriter.WriteHeader(header)")
	}

	_, err = tarWriter.Write(headerBytes)
	if err != nil {
		return errors.Wrap(err, "tarWriter.Write(header)")
	}

	err = tarWriter.WriteHeader(&tar.Header{
		Name: "data",
		Mode: int64(fstat.Mode()),
		Size: int64(fstat.Size()),
	})

	if err != nil {
		return errors.Wrap(err, "tarWriter.WriteHeader")
	}

	_, err = io.Copy(tarWriter, f)
	if err != nil {
		return errors.Wrap(err, "failed to read file")
	}
	tarWriter.Close()
	gzipWriter.Close()

	_, err = b.s3Cli.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(b.s3Bucket),
		Key:    aws.String(storeKey(hash)),
		Body:   bytes.NewReader(gzippedBuf.Bytes()),
	})
	if err != nil {
		return errors.Wrap(err, "failed to write file in s3")
	}

	hashFile := path + ".sha1"

	err = ioutil.WriteFile(hashFile, []byte(hash), 0644)
	if err != nil {
		return errors.Wrapf(err, "failed to create hash file %q", hashFile)
	}

	return nil
}

func (b *s3Bin) Get(sha1File string) error {
	targetFile := strings.TrimSuffix(sha1File, ".sha1")
	if targetFile == sha1File {
		return errors.New("SHA1 file doesn't have .sha1 extension")
	}

	sha1Bytes, err := ioutil.ReadFile(sha1File)
	if err != nil {
		return errors.Wrapf(err, "failed to read sha1 file %q", sha1File)
	}

	sha1Str := strings.ToLower(strings.TrimSpace(string(sha1Bytes)))
	if len(sha1Str) != 40 {
		return errors.Wrapf(err, "sha1 file %q is invalid", sha1File)
	}

	existingHash, err := calcSha1(targetFile)
	if err == nil {
		if existingHash == sha1Str {
			log.Printf("%q exists and is up-to-date", targetFile)
			return nil
		} else {
			log.Printf("Updating %q", targetFile)
		}
	} else if os.IsNotExist(errors.Cause(err)) {
		log.Printf("Downloading %q", targetFile)
	} else {
		return err
	}

	key := storeKey(sha1Str)

	res, err := b.s3Cli.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(b.s3Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return errors.Wrapf(err, "failed to read %q from S3 bucket %q",
			key, b.s3Bucket)
	}
	defer res.Body.Close()

	gzipReader, err := gzip.NewReader(res.Body)
	if err != nil {
		return errors.Wrap(err, "failed to create gzip reader")
	}

	tarReader := tar.NewReader(gzipReader)
	tarHdr, err := tarReader.Next()
	if err != nil {
		return errors.Wrap(err, "tarReader.Next")
	}

	if tarHdr.Name != "header" {
		return errors.New("tar does not have 'header'")
	}

	headerBytes, err := ioutil.ReadAll(tarReader)
	if err != nil {
		return errors.Wrap(err, "failed to read header")
	}

	var header Header
	err = json.Unmarshal(headerBytes, &header)
	if err != nil {
		return errors.Wrap(err, "json.Unmarshal")
	}

	if header.Version != version {
		return errors.Wrapf(err, "unsupported version %d", header.Version)
	}

	tarHdr, err = tarReader.Next()
	if err != nil {
		return errors.Wrap(err, "tarReader.Next")
	}

	if tarHdr.Name != "data" {
		return errors.Errorf("tar does not have 'data'")
	}

	f, err := os.Create(targetFile)
	if err != nil {
		return errors.Wrapf(err, "failed to create target file %q", targetFile)
	}
	defer f.Close()

	_, err = io.Copy(f, tarReader)
	if err != nil {
		return errors.Wrapf(err, "failed to copy file")
	}

	err = f.Chmod(os.FileMode(tarHdr.Mode))
	if err != nil {
		return errors.Wrap(err, "failed to set file mode")
	}

	return nil
}

func (b *s3Bin) GetDir(root string) error {
	return filepath.Walk(
		root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() && path != root && info.Name() != "." && info.Name() != ".." {
				return b.GetDir(path)
			}

			if filepath.Ext(path) != ".sha1" {
				return nil
			}

			return b.Get(path)
		})
}

func calcSha1(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", errors.Wrap(err, "failed to open file")
	}
	defer f.Close()

	hash := sha1.New()
	_, err = io.Copy(hash, f)
	if err != nil {
		return "", errors.Wrap(err, "failed to read file")
	}

	return strings.ToLower(hex.EncodeToString(hash.Sum(nil))), nil
}

func storeKey(hash string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s",
		hash[:4], hash[4:8], hash[8:12], hash[12:16], hash[16:20])
}

func main() {
	var (
		flagS3Bucket  = flag.String("s3-bucket", "", "`name` of S3 bucket where binaries are stored")
		flagAWSRegion = flag.String("aws-region", "", "S3 bucket's `AWS region`")
		flagGet       = flag.String("get", "", "download file given corresponding `sha1 file`")
		flagGetDir    = flag.String("get-dir", "", "download all files in `directory`")
		flagPut       = flag.String("put", "", "put `file` in S3 and create corresponding .sha1 file")
	)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "s3bin [options] -get <file.sha1>\n")
		fmt.Fprintf(os.Stderr, "s3bin [options] -get-dir <directory>\n")
		fmt.Fprintf(os.Stderr, "s3bin [options] -put <file>\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "s3bin downloads or uploads binary files from/to a AWS S3 bucket. \n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "With the -put flag, s3bin uploads the file to the S3 bucket, and creates a \n")
		fmt.Fprintf(os.Stderr, "file with the same name plus the .sha1 extension. This file will contain the \n")
		fmt.Fprintf(os.Stderr, "SHA1 hash of the uploaded binary.\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "With the -get flag, s3bin takes the sha1 file created by -put and downloads \n")
		fmt.Fprintf(os.Stderr, "the corresponding file from S3 iff the corresponding local file dos not exist \n")
		fmt.Fprintf(os.Stderr, "or its contents do not match the provided hash.\n")
		fmt.Fprintf(os.Stderr, "\n")
		os.Exit(1)
	}

	flag.Parse()

	log.SetFlags(0)

	if *flagS3Bucket == "" {
		log.Println("-s3-bucket is required")
		flag.Usage()
	}

	if *flagAWSRegion == "" {
		log.Println("-aws-region is required")
		flag.Usage()
	}

	if *flagGet == "" && *flagGetDir == "" && *flagPut == "" {
		flag.Usage()
	}

	s3Bin, err := newS3Bin(*flagAWSRegion, *flagS3Bucket)
	if err != nil {
		log.Fatal(err)
	}

	if *flagGet != "" {
		err = s3Bin.Get(*flagGet)
		if err != nil {
			log.Fatal(err)
		}
	} else if *flagGetDir != "" {
		err = s3Bin.GetDir(*flagGetDir)
		if err != nil {
			log.Fatal(err)
		}
	} else if *flagPut != "" {
		err = s3Bin.Put(*flagPut)
		if err != nil {
			log.Fatal(err)
		}
	}
}

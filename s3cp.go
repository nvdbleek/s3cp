package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	s "strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var s3URLRegExp, _ = regexp.Compile("^s3://([^/]+)/?(.*)")

func main() {
	aclFlag := flag.String("acl", "private", "Sets  the  ACL for the object when the command is per-formed. If you use this parameter you must have  the  \"s3:PutObjectAcl\" permission  included  in  the list of actions for your IAM policy. Only accepts values of private,  public-read,  public-read-write,  authenti-cated-read, aws-exec-read, bucket-owner-read, bucket-owner-full-control and log-delivery-write. See Canned ACL for details")
	flag.Parse()

	if len(flag.Args()) < 2 {
		fmt.Printf("usage: %s <filePath> <s3URL>\n", os.Args[0])
		os.Exit(1)
	}

	args := flag.Args()

	from := args[0]
	to := args[1]

	match := s3URLRegExp.FindStringSubmatch(from)
	if match != nil {
		bucketName := match[1]
		key := match[2]

		if key == "" {
			fmt.Printf("Key missing in s3 URL\n")
			os.Exit(1)
		}

		s, err := session.NewSession()
		if err != nil {
			log.Fatal(err)
		}

		// Download
		err = GetFileFromS3(s, bucketName, key, to)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		return
	}

	match = s3URLRegExp.FindStringSubmatch(to)
	if match != nil {
		bucketName := match[1]
		path := match[2]
		key := path
		if path == "" || s.HasSuffix(path, "/") {
			key = path + filepath.Base(from)
		}

		s, err := session.NewSession()
		if err != nil {
			log.Fatal(err)
		}

		// Upload
		err = AddFileToS3(s, from, bucketName, key, *aclFlag)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("either from or to should be an s3 URL \n")
	os.Exit(1)

}

// AddFileToS3 will upload a single file to S3.
func AddFileToS3(s *session.Session, fileName string, bucketName string, key string, acl string) error {

	// Open the file for use
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size and read the file content into a buffer
	fileInfo, _ := file.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	file.Read(buffer)

	// Config settings: this is where you choose the bucket, filename, content-type etc.
	// of the file you're uploading.
	_, err = s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket:             aws.String(bucketName),
		Key:                aws.String(key),
		ACL:                aws.String(acl),
		Body:               bytes.NewReader(buffer),
		ContentLength:      aws.Int64(size),
		ContentType:        aws.String(http.DetectContentType(buffer)),
		ContentDisposition: aws.String("attachment"),
		// ServerSideEncryption: aws.String("AES256"),
	})
	return err
}

// GetFileFromS3 will download a single file from S3.
func GetFileFromS3(s *session.Session, bucketName string, key string, fileName string) error {

	// Open the file for use
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	downloader := s3manager.NewDownloader(s)

	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})

	return err

}

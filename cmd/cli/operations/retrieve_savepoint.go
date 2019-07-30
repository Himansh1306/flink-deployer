package operations

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/afero"
	"log"
	"net/url"
	"strings"
	"time"
)

var s3Schemes = map[string]bool{
	"s3":  true,
	"s3a": true,
	"s3p": true,
}

func (o RealOperator) retrieveLatestSavepoint(dir string) (string, error) {
	u, err := url.Parse(dir)

	if err == nil && s3Schemes[u.Scheme] {
		return o.retrieveLatestSavepointS3(u)
	}

	return o.retrieveLatestSavepointLocal(dir)
}

func (o RealOperator) retrieveLatestSavepointS3(dir *url.URL) (string, error) {
	config, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return "", errors.New("unable to load SDK config: " + err.Error())
	}

	client := s3.New(config)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(dir.Host),
	}

	if dir.Path != "" {
		input.Prefix = aws.String(strings.TrimLeft(dir.Path, "/"))
	}

	var newestFile url.URL
	var newestTime time.Time

	req := client.ListObjectsV2Request(input)
	result, err := req.Send(context.Background())
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				log.Println("s3 no such bucket failed error: ", aerr.Error())
			default:
				log.Println("s3 ListObjectsV2Request requests failed: ", aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Println("s3 ListObjectsV2Request requests failed: ", err.Error())
		}

		return "", errors.New("listing S3 objects: " + err.Error())
	}

	if len(result.Contents) > 0 {
		for _, object := range result.Contents {
			if strings.HasSuffix(*object.Key, "_metadata") && object.LastModified.After(newestTime) {
				newestTime = *object.LastModified
				newestFile = url.URL{Scheme: dir.Scheme, Host: dir.Host, Path: *object.Key}
			}
		}
	}

	return newestFile.String(), nil
}

func (o RealOperator) retrieveLatestSavepointLocal(dir string) (string, error) {
	if strings.HasSuffix(dir, "/") {
		dir = strings.TrimSuffix(dir, "/")
	}

	files, err := afero.ReadDir(o.Filesystem, dir)
	if err != nil {
		return "", err
	}

	if len(files) == 0 {
		return "", errors.New("No savepoints present in directory: " + dir)
	}

	var newestFile string
	var newestTime int64
	for _, f := range files {
		filePath := dir + "/" + f.Name()
		fi, err := o.Filesystem.Stat(filePath)
		if err != nil {
			return "", err
		}
		currTime := fi.ModTime().Unix()
		if currTime > newestTime {
			newestTime = currTime
			newestFile = filePath
		}
	}

	return newestFile, nil
}

package itswizard_aws

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
	"sort"
	"time"
)

type Bucket struct {
	session *session.Session
	service *s3.S3
	region  string
	path    string
	name    string
}

type timeSlice []time.Time

func (s timeSlice) Less(i, j int) bool { return s[i].Before(s[j]) }
func (s timeSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s timeSlice) Len() int           { return len(s) }

/*
When the region string is empty. Default is: eu-central-1
*/
func NewBucket(region string) (out *Bucket, err error) {
	a := new(Bucket)
	if region == "" {
		a.region = "eu-central-1"
	} else {
		a.region = region
	}
	sess, err := session.NewSession(&aws.Config{Region: aws.String(a.region)})
	if err != nil {
		return nil, err
	}
	a.session = sess
	svc := s3.New(a.session)
	a.service = svc
	return a, nil
}

/*
sets the path where the file should be stored or where the files can be downloaded
*/
func (p *Bucket) SetPath(path string) {
	p.path = path
}

/*
sets the name of the bucket
*/
func (p *Bucket) SetName(name string) {
	p.name = name
}

/*
Todo: Error !!!!!
creates the bucket in aws
*/
func (p *Bucket) Create() error {
	// Create the S3 Bucket
	_, err := p.service.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(p.name),
	})
	if err != nil {
		return err
	}
	err = p.service.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(p.name),
	})
	if err != nil {
		return err
	}
	return nil
}

/*
This function upload a file to the s3 Bucket.
Important the path has to end with "/"
Declare the postfix without the dot!
*/
func (p *Bucket) UploadAFile(filepath string, filename string) error {
	file, err := os.Open(fmt.Sprint(filepath, filename))
	if err != nil {
		return err
	}
	uploader := s3manager.NewUploader(p.session)
	upParams := &s3manager.UploadInput{Bucket: &p.name, Key: &filename, Body: file}
	_, err = uploader.Upload(upParams)
	if err != nil {
		return err
	}
	return nil
}

/*
Listed the files in the bucket with the set path
*/
func (p *Bucket) Ls() (m map[time.Time]string, err error) {
	resp, err := p.service.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(p.name),
		Prefix: aws.String(p.path),
	})
	if err != nil {
		return nil, err
	}

	m = make(map[time.Time]string)
	for _, item := range resp.Contents {
		m[*item.LastModified] = *item.Key
	}
	return
}

/*
Returns the filename of the last uploaded file in the directory
*/
func (p *Bucket) GetTheLatestUploadedFile() (lastFile string, err error) {
	var dateSlice timeSlice = []time.Time{}

	allFiles, err := p.Ls()
	if err != nil {
		return "", err
	}

	for t, _ := range allFiles {
		dateSlice = append(dateSlice, t)
	}

	sort.Sort(sort.Reverse(dateSlice))

	if len(dateSlice) > 0 {
		lastFile = allFiles[dateSlice[0]]
	} else {
		return "", errors.New(fmt.Sprint("There is no file in the bucket " + p.name))
	}
	return
}

/*
Download a File
*/
func (p *Bucket) DownloadAFile(filename string) (out []byte, err error) {

	buffer := aws.NewWriteAtBuffer([]byte(""))

	downloader := s3manager.NewDownloader(p.session)
	_, err = downloader.Download(buffer,
		&s3.GetObjectInput{
			Bucket: aws.String(p.name),
			Key:    aws.String(filename),
		})
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

/*
taregetpath must have a "/" at the end!
*/
func (p *Bucket) DownloadAFileAndStore(filename string, targetpath string) error {
	file, err := os.Create(fmt.Sprint(targetpath, filename))
	if err != nil {
		return err
	}
	defer file.Close()
	downloader := s3manager.NewDownloader(p.session)
	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(p.name),
			Key:    aws.String(filename),
		})
	if err != nil {
		return err
	}
	return nil
}

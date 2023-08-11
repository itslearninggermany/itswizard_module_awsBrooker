package itswizard_module_awsBrooker

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type timeSlice []time.Time

func (s timeSlice) Less(i, j int) bool { return s[i].Before(s[j]) }
func (s timeSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s timeSlice) Len() int           { return len(s) }

func CreateANewBucket(bucketName string) (success bool, log string) {
	success = true
	sess, _ := session.NewSession(&aws.Config{Region: aws.String("eu-central-1")})
	svc := s3.New(sess)

	// Create the S3 Bucket
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		log = log + fmt.Sprintf("Unable to create bucket %q, %v", bucketName, err)
		success = false
		return
	}

	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		log = log + fmt.Sprintf("Error occurred while waiting for bucket to be created, %v", bucketName)
		success = false
		return
	}

	log = log + fmt.Sprintf("Bucket %q successfully created\n", bucketName)
	return
}

// This function upload a file to the s3 Bucket.
// Important the path has to end with "/"
// Declare the postfix without the dot!
func UploadAFile(path string, bucketName string, folder string, postfix string, filename string) (log string) {
	name := filename
	if filename == "" {
		min := 0
		max := 100000
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		newId := r.Intn(max-min) + min
		name = strconv.Itoa(newId)
	}
	keyName := folder + name
	if postfix != "" {
		keyName = keyName + "." + postfix
	}

	sess, _ := session.NewSession(&aws.Config{Region: aws.String("eu-central-1")})
	file, err := os.Open(path)
	if err != nil {
		log = "Problem by opening file " + path + " /n"
	}

	uploader := s3manager.NewUploader(sess)

	upParams := &s3manager.UploadInput{Bucket: &bucketName, Key: &keyName, Body: file}

	result, err := uploader.Upload(upParams)

	if err != nil {
		log = log + fmt.Sprint(err)
	}

	log = log + result.UploadID + " was stored in s3Bucket " + bucketName
	return
}

func ListAllDataOfBucket(bucketName string, path string) (m map[time.Time]string, log string) {
	sess, _ := session.NewSession(&aws.Config{Region: aws.String("eu-central-1")})

	svc := s3.New(sess)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(path),
	})
	if err != nil {
		log = fmt.Sprintln("Unable to list items in bucket ", bucketName, err)
	}

	m = make(map[time.Time]string)
	for _, item := range resp.Contents {
		m[*item.LastModified] = *item.Key
	}
	return
}

func GetTheLatestUploadedFile(bucketName string, path string) (lastFile, log string) {
	var dateSlice timeSlice = []time.Time{}

	allFiles, log := ListAllDataOfBucket(bucketName, path)

	for t, _ := range allFiles {
		dateSlice = append(dateSlice, t)
	}

	sort.Sort(sort.Reverse(dateSlice))

	if len(dateSlice) > 0 {
		lastFile = allFiles[dateSlice[0]]
	} else {
		log = log + "/n" + "There is no file in the bucket " + bucketName
	}

	return
}

func DownloadTheLastUploadedFile(bucketName string, path string, targetpath string) (filename string, log string) {
	sess, _ := session.NewSession(&aws.Config{Region: aws.String("eu-central-1")})
	filename, log = GetTheLatestUploadedFile(bucketName, path)

	tmp := strings.Split(filename, "/")

	file, err := os.Create(targetpath + tmp[len(tmp)-1])
	if err != nil {
		log = log + "/n" + "error by creating filename on ec2 from bucket " + bucketName + " filename: " + tmp[len(tmp)-1]
	}

	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(filename),
		})
	if err != nil {
		prob := fmt.Sprintln("Unable to download item %q, %v", err)
		log = log + "/n" + prob
	}

	log = log + "/n" + fmt.Sprintln("Downloaded", numBytes, "bytes")
	filename = tmp[len(tmp)-1]
	return
}

func DownloadFileFromBucket(bucketName string, filename string) (content []byte, log string) {
	sess, _ := session.NewSession(&aws.Config{Region: aws.String("eu-central-1")})
	/*
		min := 0
		max := 100000
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		newId := r.Intn(max-min) + min
		keyName := strconv.Itoa(newId)
	*/

	// Todo: Schauen, ob es diese Datei schon gibt.

	s := strings.Split(filename, "/")

	file, err := os.Create(s[len(s)-1])
	if err != nil {
		log = log + "/n" + "error by creating filename on ec2 from bucket " + bucketName + " filename: " + s[len(s)-1]
	}

	downloader := s3manager.NewDownloader(sess)
	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(filename),
		})

	if err != nil {
		prob := fmt.Sprintln("Unable to download item %q, %v", err)
		log = log + "/n" + prob
	}

	content, err = ioutil.ReadFile(s[len(s)-1])

	if err != nil {
		log = log + fmt.Sprint(err)
	}

	return
}

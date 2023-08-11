package itswizard_aws

import "net/http"

import "fmt"

/*
Returns the instance ID of the ec2 in a string.
*/
func GetInstance() string {
	resp, err := http.Get("http://169.254.169.254/latest/meta-data/instance-id")
	if err != nil {
		fmt.Println(err)
		return err.Error()
	}

	bs := make([]byte, resp.ContentLength)
	resp.Body.Read(bs)
	resp.Body.Close()
	return string(bs)
}

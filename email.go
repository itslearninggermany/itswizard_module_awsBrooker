package itswizard_aws

import (
	"crypto/tls"
	"fmt"
	"github.com/jinzhu/gorm"
	"net"
	"net/mail"
	"net/smtp"
	"strconv"
)

type DbEmailServerData15 struct {
	gorm.Model
	SmtpServer string
	Port       uint
	Password   string
	Username   string
}

type AwsEmail struct {
	fromEmail  string
	toEmail    string
	subject    string
	body       string
	emailSetup DbEmailServerData15
}

func NewAwsEmail(fromEmail string, toEmail string, subject string, body string, emailSetup DbEmailServerData15) *AwsEmail {
	a := new(AwsEmail)
	a.fromEmail = fromEmail
	a.toEmail = toEmail
	a.subject = subject
	a.body = body
	a.emailSetup = emailSetup
	return a
}

func (p *AwsEmail) Send() error {
	from := mail.Address{"", p.fromEmail}
	to := mail.Address{"", p.toEmail}

	subj := p.subject
	body := p.body

	// Setup headers
	headers := make(map[string]string)
	headers["From"] = from.String()
	headers["To"] = to.String()
	headers["Subject"] = subj

	// Setup message
	message := ""
	for k, v := range headers {
		message += fmt.Sprintf("%s: %s\r\n", k, v)
	}
	message += "\r\n" + body
	// Connect to the SMTP Server

	port := strconv.Itoa(int(p.emailSetup.Port))
	servername := p.emailSetup.SmtpServer + ":" + port
	host, _, _ := net.SplitHostPort(servername)
	auth := smtp.PlainAuth("", p.emailSetup.Username, p.emailSetup.Password, host)

	// TLS config
	tlsconfig := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         host,
	}

	// Here is the key, you need to call tls.Dial instead of smtp.Dial
	// for smtp servers running on 465 that require an ssl connection
	// from the very beginning (no starttls)
	conn, err := tls.Dial("tcp", servername, tlsconfig)
	if err != nil {
		return err
	}

	c, err := smtp.NewClient(conn, host)
	if err != nil {
		return err
	}

	// Auth
	if err = c.Auth(auth); err != nil {
		return err
	}

	// To && From
	if err = c.Mail(from.Address); err != nil {
		return err
	}

	if err = c.Rcpt(to.Address); err != nil {
		return err
	}

	// Data
	w, err := c.Data()
	if err != nil {
		return err
	}

	_, err = w.Write([]byte(message))
	if err != nil {
		return err
	}

	err = w.Close()
	if err != nil {
		return err
	}

	err = c.Quit()
	if err != nil {
		return err
	}

	return nil
}

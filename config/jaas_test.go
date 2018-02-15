package config

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestExtractsJaasCredentials(t *testing.T) {
	jaas := `
		KafkaClient {
		  org.apache.kafka.common.security.plain.PlainLoginModule required
		  username="alice"
		  password="veyaiThai5que0ieb5le";
		};
	`
	credentials, err := NewJaasCredentials(jaas)
	a := assert.New(t)
	a.Nil(err)
	a.Equal("alice", credentials.Username)
	a.Equal("veyaiThai5que0ieb5le", credentials.Password)
}

func TestExtractsJaasCredentialsFromFile(t *testing.T) {
	jaas := `
		KafkaClient {
		  org.apache.kafka.common.security.plain.PlainLoginModule required
		  username="alice"
		  password="veyaiThai5que0ieb5le";
		};
	`
	tmpFile, err := ioutil.TempFile("", "kafka-proxy-jaas-test")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	_, err = tmpFile.WriteString(jaas)
	assert.Nil(t, err)

	credentials, err := NewJaasCredentialFromFile(tmpFile.Name())
	a := assert.New(t)
	a.Nil(err)
	a.Equal("alice", credentials.Username)
	a.Equal("veyaiThai5que0ieb5le", credentials.Password)

}

func TestExtractsJaasWithBlankCredentials(t *testing.T) {
	jaas := `
		KafkaClient {
		  org.apache.kafka.common.security.plain.PlainLoginModule required
		  username   = "alice"
		  password	= "veyaiThai5que0ieb5le";
		};
	`
	credentials, err := NewJaasCredentials(jaas)
	a := assert.New(t)
	a.Nil(err)
	a.Equal("alice", credentials.Username)
	a.Equal("veyaiThai5que0ieb5le", credentials.Password)
}

func TestJaasPasswordMissingError(t *testing.T) {
	jaas := `
		KafkaClient {
		  org.apache.kafka.common.security.plain.PlainLoginModule required
		  username = "alice"
		};
	`
	_, err := NewJaasCredentials(jaas)
	a := assert.New(t)
	a.EqualError(err, "cannot retrieve jaas password: no entry was found")
}

func TestJaasUsernameMissingError(t *testing.T) {
	jaas := `
		KafkaClient {
		  org.apache.kafka.common.security.plain.PlainLoginModule required
		  password="veyaiThai5que0ieb5le";
		};
	`
	_, err := NewJaasCredentials(jaas)
	a := assert.New(t)
	a.EqualError(err, "cannot retrieve jaas username: no entry was found")
}

func TestJaas2UsersMultiLineError(t *testing.T) {
	jaas := `
		KafkaClient {
		  org.apache.kafka.common.security.plain.PlainLoginModule required
		  username = "alice"
		  username = "alice2"
	   	  password="veyaiThai5que0ieb5le";
		};
	`
	_, err := NewJaasCredentials(jaas)
	a := assert.New(t)
	a.EqualError(err, "cannot retrieve jaas username: multiple entries were found")
}
func TestJaas2UsersOneLineError(t *testing.T) {
	jaas := `
		KafkaClient {
		  org.apache.kafka.common.security.plain.PlainLoginModule required
		  username = "alice"  username = "alice2"
	   	  password="veyaiThai5que0ieb5le";
		};
	`
	_, err := NewJaasCredentials(jaas)
	a := assert.New(t)
	a.EqualError(err, "cannot retrieve jaas username: multiple entries were found")
}

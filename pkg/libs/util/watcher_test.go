package util

import (
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatchRegularFileChange(t *testing.T) {
	t.Skip() // Uncomment to execute

	a := assert.New(t)

	dirName, err := os.MkdirTemp("", "watcher-test-")
	a.Nil(err)
	defer os.Remove(dirName)

	targetSecret, err := os.CreateTemp(dirName, "secret-")
	a.Nil(err)
	defer os.Remove(targetSecret.Name())

	_, err = targetSecret.WriteString("secret1")
	a.Nil(err)

	data, err := os.ReadFile(targetSecret.Name())
	a.Nil(err)
	a.Equal("secret1", string(data))

	done := make(chan bool)
	defer close(done)

	var ops int32
	actions := make(chan bool, 256)

	action := func() {
		logrus.Infof("Action")
		atomic.AddInt32(&ops, 1)
		actions <- true
	}

	err = WatchForUpdates(targetSecret.Name(), done, action)
	a.Nil(err)
	logrus.Println("Waitning for file update")
	time.Sleep(1 * time.Second)

	_, err = targetSecret.WriteString("addition")
	a.Nil(err)

	select {
	case <-actions:
	case <-time.After(5 * time.Second):
	}

	opsFinal := atomic.LoadInt32(&ops)
	a.Equal(int32(1), opsFinal)

	data, err = os.ReadFile(targetSecret.Name())
	a.Nil(err)
	a.Equal("secret1addition", string(data))
}

func TestWatchLinkedFileChange(t *testing.T) {
	t.Skip() // Uncomment to execute

	/* Kubernetes way
	drwxr-xr-x    2 root     root            60 Mar 20 15:56 ..3983_20_03_15_56_28.934673085
	lrwxrwxrwx    1 root     root            31 Mar 20 15:56 ..data -> ..3983_20_03_15_56_28.934673085
	lrwxrwxrwx    1 root     root            24 Mar 20 15:56 ca-chain.cert.pem -> ..data/ca-chain.cert.pem
	*/
	a := assert.New(t)

	dirName, err := os.MkdirTemp("", "watcher-test-")
	a.Nil(err)
	defer os.Remove(dirName)

	dirTmp1, err := os.MkdirTemp(dirName, "tmp1-")
	a.Nil(err)
	defer os.Remove(dirTmp1)

	dirTmp2, err := os.MkdirTemp(dirName, "tmp2-")
	a.Nil(err)
	defer os.Remove(dirTmp2)

	targetSecret1, err := os.CreateTemp(dirTmp1, "secret-")
	a.Nil(err)
	defer os.Remove(targetSecret1.Name())

	targetSecret2, err := os.CreateTemp(dirTmp2, "secret-")
	a.Nil(err)
	defer os.Remove(targetSecret2.Name())

	/* write some data */
	_, err = targetSecret1.WriteString("secret1")
	a.Nil(err)
	_, err = targetSecret2.WriteString("secret2")
	a.Nil(err)

	dataLink := filepath.Join(dirName, "data")
	err = os.Symlink(targetSecret1.Name(), dataLink)
	a.Nil(err)
	defer os.Remove(dataLink)

	secretLink := filepath.Join(dirName, "secret.txt")
	err = os.Symlink(dataLink, secretLink)
	a.Nil(err)
	defer os.Remove(secretLink)

	data, err := os.ReadFile(secretLink)
	a.Nil(err)
	a.Equal("secret1", string(data))

	done := make(chan bool)
	defer close(done)

	var ops int32
	actions := make(chan bool, 256)

	action := func() {
		logrus.Infof("Action")
		atomic.AddInt32(&ops, 1)
		actions <- true
	}

	err = WatchForUpdates(secretLink, done, action)
	a.Nil(err)
	logrus.Println("Waitning for link update")
	time.Sleep(1 * time.Second)

	// remove and recreate a new link
	os.Remove(dataLink)
	err = os.Symlink(targetSecret2.Name(), dataLink)
	a.Nil(err)
	defer os.Remove(dataLink)

	select {
	case <-actions:
	case <-time.After(5 * time.Second):
	}

	opsFinal := atomic.LoadInt32(&ops)
	a.Equal(int32(1), opsFinal)

	data, err = os.ReadFile(secretLink)
	a.Nil(err)
	a.Equal("secret2", string(data))
}

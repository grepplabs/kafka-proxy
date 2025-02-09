package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
	"time"
)

type DeadlineReadWriteCloser interface {
	io.ReadWriteCloser
	SetWriteDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetDeadline(t time.Time) error
}

type DeadlineWriter interface {
	io.Writer
	SetWriteDeadline(t time.Time) error
}

type DeadlineReader interface {
	io.Reader
	SetReadDeadline(t time.Time) error
}

type DeadlineReaderWriter interface {
	DeadlineReader
	DeadlineWriter
	SetDeadline(t time.Time) error
}

// myCopy is similar to io.Copy, but reports whether the returned error was due
// to a bad read or write. The returned error will never be nil
// nolint:unused
func myCopy(dst io.Writer, src io.Reader) (readErr bool, err error) {
	buf := make([]byte, 4096)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				if err == nil {
					return false, werr
				}
				// Read and write error; just report read error (it happened first).
				return true, err
			}
		}
		if err != nil {
			return true, err
		}
	}
}

// myCopyN is similar to io.CopyN, but reports whether the returned error was due
// to a bad read or write. The returned error will never be nil
func myCopyN(dst io.Writer, src io.Reader, size int64, buf []byte) (readErr bool, err error) {
	// limit reader  - EOF when finished
	src = io.LimitReader(src, size)

	var written int64
	var n int
	for {
		n, err = src.Read(buf)
		if n > 0 {
			nw, werr := dst.Write(buf[0:n])
			if nw > 0 {
				written += int64(nw)
			}
			if err != nil {
				// Read and write error; just report read error (it happened first).
				readErr = true
				break
			}
			if werr != nil {
				err = werr
				break
			}
			if n != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if err != nil {
			readErr = true
			break
		}
	}

	if written == size {
		return false, nil
	}
	if written < size && err == nil {
		// src stopped early; must have been EOF.
		readErr = true
		err = io.EOF
	}
	return
}

func copyError(readDesc, writeDesc string, readErr bool, err error) {
	var desc string
	if readErr {
		desc = "Reading data from " + readDesc
	} else {
		desc = "Writing data to " + writeDesc
	}
	logrus.Infof("%v had error: %s", desc, err.Error())
}

func copyThenClose(cfg ProcessorConfig, remote, local DeadlineReadWriteCloser, brokerAddress string, remoteDesc, localDesc string) {

	processor := newProcessor(cfg, brokerAddress)

	firstErr := make(chan error, 1)

	go withRecover(func() {
		readErr, err := processor.RequestsLoop(remote, local)
		select {
		case firstErr <- err:
			if readErr && err == io.EOF {
				logrus.Infof("Client closed %v", localDesc)
			} else {
				copyError(localDesc, remoteDesc, readErr, err)
			}
			remote.Close()
			local.Close()
		default:
		}
	})

	readErr, err := processor.ResponsesLoop(local, remote)
	select {
	case firstErr <- err:
		if readErr && err == io.EOF {
			logrus.Infof("Server %v closed connection", remoteDesc)
		} else {
			copyError(remoteDesc, localDesc, readErr, err)
		}
		remote.Close()
		local.Close()
	default:
		// In this case, the other goroutine exited first and already printed its
		// error (and closed the things).
	}
}

// NewConnSet initializes a new ConnSet and returns it.
func NewConnSet() *ConnSet {
	return &ConnSet{m: make(map[string][]net.Conn)}
}

// A ConnSet tracks net.Conns associated with a provided ID.
type ConnSet struct {
	sync.RWMutex
	m map[string][]net.Conn
}

// String returns a debug string for the ConnSet.
func (c *ConnSet) String() string {
	var b bytes.Buffer

	c.RLock()
	for id, conns := range c.m {
		fmt.Fprintf(&b, "ID %s:", id)
		for i, c := range conns {
			fmt.Fprintf(&b, "\n\t%d: %v", i, c)
		}
	}
	c.RUnlock()

	return b.String()
}

// Add saves the provided conn and associates it with the given string
// identifier.
func (c *ConnSet) Add(id string, conn net.Conn) {
	c.Lock()
	c.m[id] = append(c.m[id], conn)
	c.Unlock()
}

// IDs returns a slice of all identifiers which still have active connections.
func (c *ConnSet) IDs() []string {
	ret := make([]string, 0, len(c.m))

	c.RLock()
	for k := range c.m {
		ret = append(ret, k)
	}
	c.RUnlock()

	return ret
}

// Conns returns all active connections associated with the provided ids.
func (c *ConnSet) Conns(ids ...string) []net.Conn {
	var ret []net.Conn

	c.RLock()
	for _, id := range ids {
		ret = append(ret, c.m[id]...)
	}
	c.RUnlock()

	return ret
}

// Count returns number of connection pro identifier
func (c *ConnSet) Count() map[string]int {
	ret := make(map[string]int)

	c.RLock()
	for k, v := range c.m {
		ret[k] = len(v)
	}
	c.RUnlock()

	return ret
}

// brokerToCount := make(map[string]int)

// Remove undoes an Add operation to have the set forget about a conn. Do not
// Remove an id/conn pair more than it has been Added.
func (c *ConnSet) Remove(id string, conn net.Conn) error {
	c.Lock()
	defer c.Unlock()

	pos := -1
	conns := c.m[id]
	for i, cc := range conns {
		if cc == conn {
			pos = i
			break
		}
	}

	if pos == -1 {
		return fmt.Errorf("couldn't find connection %v for id %s", conn, id)
	}

	if len(conns) == 1 {
		delete(c.m, id)
	} else {
		c.m[id] = append(conns[:pos], conns[pos+1:]...)
	}

	return nil
}

// Close closes every net.Conn contained in the set.
func (c *ConnSet) Close() error {
	var errs bytes.Buffer

	c.Lock()
	for id, conns := range c.m {
		for _, c := range conns {
			if err := c.Close(); err != nil {
				fmt.Fprintf(&errs, "%s close error: %v\n", id, err)
			}
		}
	}
	c.Unlock()

	if errs.Len() == 0 {
		return nil
	}

	return errors.New(errs.String())
}

func withRecover(fn func()) {
	defer func() {
		if err := recover(); err != nil {
			logrus.Errorf("Recovered from %v", err)
		}
	}()
	fn()
}

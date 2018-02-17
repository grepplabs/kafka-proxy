package proxy

import (
	"net"
	"time"
)

type TCPConnOptions struct {
	KeepAlive       time.Duration
	ReadBufferSize  int
	WriteBufferSize int
}

func (opts TCPConnOptions) setTCPConnOptions(tcpConn *net.TCPConn) error {
	if opts.KeepAlive > 0 {
		if err := tcpConn.SetKeepAlive(true); err != nil {
			return err
		}
		if err := tcpConn.SetKeepAlivePeriod(opts.KeepAlive); err != nil {
			return err
		}
	}
	if opts.ReadBufferSize > 0 {
		if err := tcpConn.SetReadBuffer(opts.ReadBufferSize); err != nil {
			return err
		}
	}
	if opts.WriteBufferSize > 0 {
		if err := tcpConn.SetWriteBuffer(opts.WriteBufferSize); err != nil {
			return err
		}
	}
	return nil
}

package proxy

import (
	"encoding/binary"
	"fmt"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"io"
	"time"
)

func copySaslAuthRequest(dst DeadlineWriter, src DeadlineReader, timeout time.Duration, buf []byte) (readErr bool, err error) {
	requestDeadline := time.Now().Add(timeout)
	err = dst.SetWriteDeadline(requestDeadline)
	if err != nil {
		return false, err
	}
	err = src.SetReadDeadline(requestDeadline)
	if err != nil {
		return true, err
	}

	sizeBuf := make([]byte, 4) // Size => int32
	if _, err = io.ReadFull(src, sizeBuf); err != nil {
		return true, err
	}

	length := binary.BigEndian.Uint32(sizeBuf)
	if int32(length) > protocol.MaxRequestSize {
		return true, protocol.PacketDecodingError{Info: fmt.Sprintf("auth message of length %d too large", length)}
	}
	//logrus.Printf("SASL auth request length %v", length)

	// write - send to broker
	if _, err = dst.Write(sizeBuf); err != nil {
		return false, err
	}
	if readErr, err = myCopyN(dst, src, int64(length), buf); err != nil {
		return readErr, err
	}
	return false, nil
}

func copySaslAuthResponse(dst DeadlineWriter, src DeadlineReader, timeout time.Duration) (readErr bool, err error) {
	//logrus.Printf("SASL auth response")

	responseDeadline := time.Now().Add(timeout)
	err = dst.SetWriteDeadline(responseDeadline)
	if err != nil {
		return false, err
	}
	err = src.SetReadDeadline(responseDeadline)
	if err != nil {
		return true, err
	}

	header := make([]byte, 4)
	_, err = io.ReadFull(src, header)
	if err != nil {
		return true, err
	}

	if _, err = dst.Write(header); err != nil {
		return false, err
	}
	return false, nil
}

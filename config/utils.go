package config

import (
	"net"
	"strconv"
)

func SplitHostPort(hostport string) (string, int32, error) {
	host, portstr, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portstr)
	if err != nil {
		return "", 0, err
	}
	return host, int32(port), nil
}

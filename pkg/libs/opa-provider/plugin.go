package opaprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/sirupsen/logrus"
)

const (
	StatusOK                  = 0
	StatusUnauthorized        = 1
	StatusMarshallFailed      = 2
	StatusAuthzRequestFailed  = 3
	StatusUnMarshallFailed    = 4
	StatusErrorReadingRequest = 5
	StatusAuthzRequestTimeout = 6
)

var (
	clockSkew = 1 * time.Minute
	nowFn     = time.Now
)

// AuthzProvider - represents AuthzProvider
type AuthzProvider struct {
	timeout  time.Duration
	authzUrl string
	l        sync.RWMutex
}

// AuthzProviderOptions - options specific for opa
type AuthzProviderOptions struct {
	Timeout  int
	AuthzUrl string
}

type OpaAuthzInput struct {
	Apikey     int32    `json:"api_key"`
	Apiversion int32    `json:"api_version"`
	UserInfo   string   `json:"user_info"`
	SrcIp      string   `json:"src_ip"`
	DstIp      string   `json:"dst_ip"`
	Topics     []string `json:"topics"`
	ClientId   string   `json:"client_id"`
}

type OpaAuthzRequest struct {
	Input OpaAuthzInput `json:"input"`
}

type OpaAuthzResponse struct {
	Result bool `json:"result"`
}

// NewAuthzProvider - Generate new opa token provider
func NewAuthzProvider(options AuthzProviderOptions) (*AuthzProvider, error) {
	AuthzProvider := &AuthzProvider{
		timeout:  time.Duration(options.Timeout) * time.Second,
		authzUrl: options.AuthzUrl,
	}

	return AuthzProvider, nil
}

// Authorize - implements apis.AuthzProvider.Authorize method
func (p *AuthzProvider) Authorize(parent context.Context, req apis.AuthzRequest) (apis.AuthzResponse, error) {
	ctx, cancel := context.WithTimeout(parent, p.timeout)
	defer cancel()
	opaInput := OpaAuthzInput{
		Apikey:     req.Apikey,
		Apiversion: req.Apiversion,
		UserInfo:   req.UserInfo,
		SrcIp:      req.SrcIp,
		DstIp:      req.DstIp,
		Topics:     strings.Split(req.Topics, ";"),
		ClientId:   req.ClientId,
	}
	opaReq := OpaAuthzRequest{}
	opaReq.Input = opaInput

	mReq, err := json.Marshal(opaReq)

	if err != nil {
		return getAuthzResponse(StatusMarshallFailed), err
	}

	httpReq, err := http.NewRequest("POST", p.authzUrl, bytes.NewBuffer(mReq))

	if err != nil {
		return getAuthzResponse(StatusErrorReadingRequest), err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.WithContext(ctx)

	respChan := make(chan apis.AuthzResponse, 0)
	go opaAuthorize(httpReq, respChan)

	select {
	case <-ctx.Done():
		logrus.Errorf("Request to authorization server %s timed-out %v", p.authzUrl, ctx.Err())
		return getAuthzResponse(StatusAuthzRequestTimeout), err
	case r := <-respChan:
		return r, err
	}
}

func opaAuthorize(httpReq *http.Request, respChan chan apis.AuthzResponse) {
	client := &http.Client{}
	opaResp := &OpaAuthzResponse{}
	resp, err := client.Do(httpReq)

	if err != nil {
		logrus.Errorf("Authorization request failed %v", err)
		respChan <- getAuthzResponse(StatusAuthzRequestFailed)
		return
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	err = json.Unmarshal(body, opaResp)

	if err != nil {
		logrus.Errorf("Unmarshalling authorization response failed %v", err)
		respChan <- getAuthzResponse(StatusUnMarshallFailed)
		return
	}

	if opaResp.Result {
		respChan <- getAuthzResponse(StatusOK)
	} else {
		respChan <- getAuthzResponse(StatusUnauthorized)
	}

	return
}

func getAuthzResponse(status int) apis.AuthzResponse {
	success := status == StatusOK
	return apis.AuthzResponse{Success: success, Status: int32(status)}
}

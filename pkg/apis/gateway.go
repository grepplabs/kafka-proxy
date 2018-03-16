package apis

type TokenRequest struct {
	Params []string
}
type TokenResponse struct {
	Success bool
	Status  int32
	Token   string
}

type TokenProvider interface {
	GetToken(request TokenRequest) (TokenResponse, error)
}

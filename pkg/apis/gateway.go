package apis

type TokenProvider interface {
	GetToken(claims []string) (int32, string, error)
}

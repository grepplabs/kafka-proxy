package apis

type PasswordAuthenticator interface {
	Authenticate(username, password string) (bool, int32, error)
}

type PasswordAuthenticatorFactory interface {
	New(params []string) (PasswordAuthenticator, error)
}

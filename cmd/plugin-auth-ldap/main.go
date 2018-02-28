package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/grepplabs/kafka-proxy/plugin/auth/shared"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"gopkg.in/ldap.v2"
	"net"
	"net/url"
	"os"
	"strings"
)

//TODO: connection connection pooling, credential caching (TTL, max number of entries), negative caching
type LdapAuthenticator struct {
	Urls      []string
	StartTLS  bool
	UPNDomain string
	UserDN    string
	UserAttr  string
}

func (pa LdapAuthenticator) Authenticate(username, password string) (bool, int32, error) {
	// logrus.Printf("Authenticate request for %s:%s,expected %s:%s ", username, password, pa.username, pa.password)
	l, err := pa.DialLDAP()
	if err != nil {
		logrus.Errorf("user %s ldap dial error %v", username, err)
		return false, 1, nil
	}
	defer l.Close()

	err = l.Bind(pa.getUserBindDN(username), password)
	if err != nil {
		if ldapErr, ok := err.(*ldap.Error); ok && ldapErr.ResultCode == ldap.LDAPResultInvalidCredentials {
			logrus.Errorf("user %s credentials are invalid", username)
			return false, 0, nil
		}
		logrus.Errorf("user %s ldap bind error %v", username, err)
		return false, 2, nil
	}
	return true, 0, nil
}

func (pa LdapAuthenticator) getUserBindDN(username string) string {

	if pa.UPNDomain != "" {
		return fmt.Sprintf("%s@%s", escapeLDAPValue(username), pa.UPNDomain)
	}
	return fmt.Sprintf("%s=%s,%s", pa.UserAttr, escapeLDAPValue(username), pa.UserDN)
}

func escapeLDAPValue(input string) string {
	// RFC4514 forbids un-escaped:
	// - leading space or hash
	// - trailing space
	// - special characters '"', '+', ',', ';', '<', '>', '\\'
	// - null
	for i := 0; i < len(input); i++ {
		escaped := false
		if input[i] == '\\' {
			i++
			escaped = true
		}
		switch input[i] {
		case '"', '+', ',', ';', '<', '>', '\\':
			if !escaped {
				input = input[0:i] + "\\" + input[i:]
				i++
			}
			continue
		}
		if escaped {
			input = input[0:i] + "\\" + input[i:]
			i++
		}
	}
	if input[0] == ' ' || input[0] == '#' {
		input = "\\" + input
	}
	if input[len(input)-1] == ' ' {
		input = input[0:len(input)-1] + "\\ "
	}
	return input
}
func (pa LdapAuthenticator) DialLDAP() (*ldap.Conn, error) {
	var retErr *multierror.Error
	var conn *ldap.Conn
	for _, uut := range pa.Urls {
		u, err := url.Parse(uut)
		if err != nil {
			retErr = multierror.Append(retErr, fmt.Errorf("error parsing url %q: %s", uut, err.Error()))
			continue
		}
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			host = u.Host
		}
		switch u.Scheme {
		case "ldap":
			if port == "" {
				port = "389"
			}
			conn, err = ldap.Dial("tcp", net.JoinHostPort(host, port))
			if err != nil {
				break
			}
			if conn == nil {
				err = fmt.Errorf("empty connection after dialing")
				break
			}
			if pa.StartTLS {
				err = conn.StartTLS(&tls.Config{InsecureSkipVerify: true})
			}
		case "ldaps":
			if port == "" {
				port = "636"
			}
			conn, err = ldap.DialTLS("tcp", net.JoinHostPort(host, port), &tls.Config{InsecureSkipVerify: true})
		default:
			retErr = multierror.Append(retErr, fmt.Errorf("invalid LDAP scheme in url %q", net.JoinHostPort(host, port)))
			continue
		}
		if err == nil {
			retErr = nil
			break
		}
		retErr = multierror.Append(retErr, fmt.Errorf("error connecting to host %q: %s", uut, err.Error()))
	}
	return conn, retErr.ErrorOrNil()
}

type pluginMeta struct {
	url       string
	startTLS  bool
	upnDomain string
	userDN    string
	userAttr  string
}

func (f *pluginMeta) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("auth plugin settings", flag.ContinueOnError)

	fs.StringVar(&f.url, "url", "", "LDAP URL to connect to (eg: ldaps://127.0.0.1:636). Multiple URLs can be specified by concatenating them with commas.")
	fs.BoolVar(&f.startTLS, "start-tls", true, "Issue a StartTLS command after establishing unencrypted connection (optional)")
	fs.StringVar(&f.upnDomain, "upn-domain", "", "Enables userPrincipalDomain login with [username]@UPNDomain (optional)")
	fs.StringVar(&f.userDN, "user-dn", "", "LDAP domain to use for users (eg: cn=users,dc=example,dc=org)")
	fs.StringVar(&f.userAttr, "user-attr", "uid", " Attribute used for users")
	return fs
}

func (f *pluginMeta) getUrls() ([]string, error) {
	result := make([]string, 0)
	urls := strings.Split(f.url, ",")
	for _, uut := range urls {
		u, err := url.Parse(uut)
		if err != nil {
			return nil, err
		}
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			host = u.Host
		}
		switch u.Scheme {
		case "ldap", "ldaps":
			result = append(result, uut)
		default:
			return nil, fmt.Errorf("invalid LDAP scheme in url %q", net.JoinHostPort(host, port))
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty LDAP url list")
	}
	return result, nil
}

func main() {

	pluginMeta := &pluginMeta{}
	flags := pluginMeta.flagSet()
	flags.Parse(os.Args[1:])

	urls, err := pluginMeta.getUrls()
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
	if pluginMeta.upnDomain == "" && (pluginMeta.userDN == "" || pluginMeta.userAttr == "") {
		logrus.Errorf("parameters user-dn and user-attr are required")
		os.Exit(1)
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"passwordAuthenticator": &shared.PasswordAuthenticatorPlugin{Impl: &LdapAuthenticator{
				Urls:      urls,
				StartTLS:  pluginMeta.startTLS,
				UPNDomain: pluginMeta.upnDomain,
				UserDN:    pluginMeta.userDN,
				UserAttr:  pluginMeta.userAttr,
			}},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}

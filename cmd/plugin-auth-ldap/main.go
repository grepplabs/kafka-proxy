package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/go-ldap/ldap/v3"
	"github.com/grepplabs/kafka-proxy/plugin/local-auth/shared"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-plugin"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net"
	"net/url"
	"os"
	"strings"
)

const UsernamePlaceholder = "%u"

type LdapAuthenticator struct {
	Urls     []string
	StartTLS bool

	UPNDomain string
	UserDN    string
	UserAttr  string

	BindDN         string
	BindPassword   string
	UserSearchBase string
	UserFilter     string
}

func (pa LdapAuthenticator) Authenticate(username, password string) (bool, int32, error) {
	// logrus.Printf("Authenticate request for %s:%s,expected %s:%s ", username, password, pa.username, pa.password)
	l, err := pa.DialLDAP()
	if err != nil {
		logrus.Errorf("user %s ldap dial error %v", username, err)
		return false, 1, nil
	}
	if l == nil {
		logrus.Errorf("ldap connection is nil")
		return false, 1, nil
	}
	defer l.Close()

	bindDN, err := pa.getUserBindDN(l, username)
	if err != nil {
		logrus.Errorf("user %s ldap get user bindDN error %v", username, err)
		return false, 1, nil
	}
	err = l.Bind(bindDN, password)
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

func (pa LdapAuthenticator) getUserBindDN(conn *ldap.Conn, username string) (string, error) {
	bindDN := ""
	if pa.BindDN != "" {
		var err error
		if pa.BindPassword != "" {
			err = conn.Bind(pa.BindDN, pa.BindPassword)
		} else {
			err = conn.UnauthenticatedBind(pa.BindDN)
		}
		if err != nil {
			return "", errors.Wrapf(err, "LDAP bind (service) failed")
		}
		searchRequest := ldap.NewSearchRequest(
			pa.UserSearchBase,
			ldap.ScopeWholeSubtree,
			ldap.NeverDerefAliases,
			0,
			0,
			false,
			strings.ReplaceAll(pa.UserFilter, UsernamePlaceholder, username),
			[]string{"dn"},
			nil,
		)
		sr, err := conn.Search(searchRequest)
		if err != nil {
			return "", err
		}
		if len(sr.Entries) < 1 {
			return "", errors.New("LDAP user search empty result")
		}
		if len(sr.Entries) > 1 {
			return "", errors.New("LDAP user search not unique result")
		}
		bindDN = sr.Entries[0].DN
	} else {
		if pa.UPNDomain != "" {
			bindDN = fmt.Sprintf("%s@%s", escapeLDAPValue(username), pa.UPNDomain)
		} else {
			bindDN = fmt.Sprintf("%s=%s,%s", pa.UserAttr, escapeLDAPValue(username), pa.UserDN)
		}
	}
	return bindDN, nil
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

	bindDN         string
	bindPassword   string
	userSearchBase string
	userFilter     string
}

func (f *pluginMeta) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("auth plugin settings", flag.ContinueOnError)

	fs.StringVar(&f.url, "url", "", "LDAP URL to connect to (eg: ldaps://127.0.0.1:636). Multiple URLs can be specified by concatenating them with commas.")
	fs.BoolVar(&f.startTLS, "start-tls", true, "Issue a StartTLS command after establishing unencrypted connection (optional)")
	fs.StringVar(&f.upnDomain, "upn-domain", "", "Enables userPrincipalDomain login with [username]@UPNDomain (optional)")
	fs.StringVar(&f.userDN, "user-dn", "", "LDAP domain to use for users (eg: cn=users,dc=example,dc=org)")
	fs.StringVar(&f.userAttr, "user-attr", "uid", " Attribute used for users")

	fs.StringVar(&f.bindDN, "bind-dn", "", "The Distinguished Name to bind to the LDAP directory to search a user. This can be a readonly or admin user")
	fs.StringVar(&f.bindPassword, "bind-passwd", "", "The password used with bindDN")
	fs.StringVar(&f.userSearchBase, "user-search-base", "", "The search base as the starting point for the user search e.g. ou=people,dc=example,dc=org")
	fs.StringVar(&f.userFilter, "user-filter", "", fmt.Sprintf("The user search filter. It must contain '%s' placeholder for the username e.g. (&(objectClass=person)(uid=%s)(memberOf=cn=kafka-users,ou=realm-roles,dc=example,dc=org))", UsernamePlaceholder, UsernamePlaceholder))

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
	if pluginMeta.bindDN != "" {
		logrus.Infof("user-search-base='%s',user-filter='%s'", pluginMeta.userSearchBase, pluginMeta.userFilter)

		if pluginMeta.userSearchBase == "" {
			logrus.Errorf("user-search-base is required")
		}
		if !strings.Contains(pluginMeta.userFilter, UsernamePlaceholder) {
			logrus.Errorf("user-filter must contain '%s' as username placeholder", UsernamePlaceholder)
		}

	} else if pluginMeta.upnDomain != "" || pluginMeta.userDN != "" {
		if pluginMeta.userDN != "" && pluginMeta.userAttr == "" {
			logrus.Errorf("parameters user-dn and user-attr are required")
			os.Exit(1)
		}
	} else {
		logrus.Errorf("parameters user-dn or bind-dn are required")
		os.Exit(1)
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"passwordAuthenticator": &shared.PasswordAuthenticatorPlugin{Impl: &LdapAuthenticator{
				Urls:           urls,
				StartTLS:       pluginMeta.startTLS,
				UPNDomain:      pluginMeta.upnDomain,
				UserDN:         pluginMeta.userDN,
				UserAttr:       pluginMeta.userAttr,
				BindDN:         pluginMeta.bindDN,
				BindPassword:   pluginMeta.bindPassword,
				UserSearchBase: pluginMeta.userSearchBase,
				UserFilter:     pluginMeta.userFilter,
			}},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}

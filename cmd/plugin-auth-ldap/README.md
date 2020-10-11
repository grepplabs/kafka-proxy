# auth-ldap plugin config

## user search and group membership check

```
make clean build plugin.auth-ldap

build/kafka-proxy server \
            --bootstrap-server-mapping "localhost:19092,0.0.0.0:30001" \
            --bootstrap-server-mapping "localhost:29092,0.0.0.0:30002" \
            --bootstrap-server-mapping "localhost:39092,0.0.0.0:30003" \
            --debug-enable \
            --auth-local-enable  \
            --auth-local-command=build/auth-ldap  \
            --auth-local-param=--url=ldap://localhost:389  \
            --auth-local-param=--ldap-ca-cert-file=/certs/ldap/ca-cert-file.pem  \
            --auth-local-param=--start-tls=false \
            --auth-local-param=--search-ldap \
            --auth-local-param=--bind-dn=cn=admin,dc=example,dc=org  \
            --auth-local-param=--bind-passwd=admin  \
            --auth-local-param=--user-search-base=ou=people,dc=example,dc=org  \
            --auth-local-param=--user-filter="(&(objectClass=person)(uid=%u)(memberOf=cn=kafka-users,ou=realm-roles,dc=example,dc=org))"
```

Setting the flag `--search-ldap` will search the user dn in LDAP, even if `--bind-dn` is not given. This is for LDAP
installations that don't need a bind before allowing readonly actions.(and therefore don't have a readony user)

If `--ldap-ca-cert-file` is set, a (chain of) certificates in PEM format needed to verify the LDAP server's identity
is read from the file given. If the flag ist not set, TLS verification can be skipped if `ldap-insecure-skip-verify` flag is true.
 


## simple user bind 

```
make clean build plugin.auth-ldap

build/kafka-proxy server \ 
            --bootstrap-server-mapping "localhost:19092,0.0.0.0:30001" \
            --bootstrap-server-mapping "localhost:29092,0.0.0.0:30002" \
            --bootstrap-server-mapping "localhost:39092,0.0.0.0:30003" \
            --debug-enable \
            --auth-local-enable  \
            --auth-local-command=build/auth-ldap  \
            --auth-local-param=--url=ldap://localhost:389  \
            --auth-local-param=--start-tls=false \
            --auth-local-param=--user-dn=ou=people,dc=example,dc=org  \
            --auth-local-param=--user-attr=uid
```            

## openldap example
### openldap setup

docker-compose.yml
```
---
version: '2'
services:
  openldap:
    ports:
      - 389:389
    image: osixia/openldap
    container_name: openldap
    volumes:
      - .:/.ldif
    environment:
      - LDAP_SEED_INTERNAL_LDIF_PATH=/.ldif
      - LDAP_TLS=false
      - LDAP_LOG_LEVEL=256
```

openldap-entries.ldif
```
dn: ou=people,dc=example,dc=org
objectClass: organizationalUnit
ou: People

dn: ou=realm-roles,dc=example,dc=org
objectclass: top
objectclass: organizationalUnit
ou: realm-roles

dn: ou=admin-roles,dc=example,dc=org
objectclass: top
objectclass: organizationalUnit
ou: admin-roles

dn: uid=jbrown,ou=people,dc=example,dc=org
objectclass: top
objectclass: person
objectclass: organizationalPerson
objectclass: inetOrgPerson
uid: jbrown
cn: James
sn: Brown
userPassword: password1

dn: uid=bwilson,ou=people,dc=example,dc=org
objectclass: top
objectclass: person
objectclass: organizationalPerson
objectclass: inetOrgPerson
uid: bwilson
cn: Bruce
sn: Wilson
userPassword: password2

dn: cn=lynch,ou=people,dc=example,dc=org
objectclass: top
objectclass: person
objectclass: organizationalPerson
objectclass: inetOrgPerson
uid: lynch
cn: Lynch
sn: Peter
userPassword: password3

dn: cn=superadmin,ou=admin-roles,dc=example,dc=org
objectclass: top
objectclass: groupOfUniqueNames
cn: accountant
uniqueMember: uid=bwilson,ou=people,dc=example,dc=org

dn: cn=kafka-users,ou=realm-roles,dc=example,dc=org
objectclass: top
objectclass: groupOfUniqueNames
cn: kafka-users
uniqueMember: uid=jbrown,ou=people,dc=example,dc=org
uniqueMember: cn=lynch,ou=people,dc=example,dc=org

dn: cn=ldap-users,ou=realm-roles,dc=example,dc=org
objectclass: top
objectclass: groupOfUniqueNames
cn: ldap-users
uniqueMember: uid=jbrown,ou=people,dc=example,dc=org
uniqueMember: cn=lynch,ou=people,dc=example,dc=org
uniqueMember: uid=bwilson,ou=people,dc=example,dc=org
```

### openldap queries

```
ldapsearch -x -LLL -H ldap://localhost:389 -D "cn=admin,dc=example,dc=org" -w admin -b "ou=people,dc=example,dc=org" "(objectClass=person)"
ldapsearch -x -LLL -H ldap://localhost:389 -D "cn=admin,dc=example,dc=org" -w admin -b "ou=people,dc=example,dc=org" "(objectClass=person)" memberOf
ldapsearch -x -H ldap://localhost:389 -D "cn=admin,dc=example,dc=org" -w admin -b "ou=people,dc=example,dc=org" "(&(objectClass=person)(uid=jbrown)(memberOf=cn=kafka-users,ou=realm-roles,dc=example,dc=org))"
```

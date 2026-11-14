# User login and company identity providers

Rayflow supports local accounts, OpenID Connect (OIDC) SSO, and LDAP / Active
Directory. Configure any combination for one company/workspace. Authenticated
users currently have full workspace access. Roles, company-to-company isolation,
self-registration, and password reset are not included. Restrict membership in
the identity provider or LDAP search filter to the intended workspace users.

Authentication is disabled by default for existing development installations.
Set `auth.enabled: true`, run `./init-db.sh` to apply the current migrations, and restart the
server. Migration 7 adds `auth_user`, `auth_session`, and `auth_login_flow`.

## Company SSO

Use a private configuration file selected by `RAYFLOW_CONFIG`:

```yaml
auth:
  enabled: true
  company_name: Example Company
  public_url: https://rayflow.example.com
  session_ttl: 8h
  oidc:
    issuer_url: https://login.example.com/realms/company
    client_id: rayflow
    client_secret: supply-through-environment
```

`public_url` is the browser-facing origin, without a path. Serve the console and
proxy `/api/*` to the server on this origin, stripping `/api`. Register this
exact callback URL in your company OIDC application:

```text
https://rayflow.example.com/api/auth/oidc/callback
```

For local development use `http://127.0.0.1:5173` (or your UI port). HTTP is
permitted only on loopback. HTTPS uses `__Host-` cookies with `Secure`, `HttpOnly`,
`SameSite=Lax`, and `Path=/`; do not rewrite their path in the reverse proxy.

Nested environment overrides use double underscores after `RAYFLOW_`:

```sh
export RAYFLOW_AUTH__ENABLED=true
export RAYFLOW_AUTH__PUBLIC_URL=https://rayflow.example.com
export RAYFLOW_AUTH__OIDC__CLIENT_SECRET='your-company-issued-secret'
```

The server discovers the issuer at startup and fails startup if it is unavailable
or invalid. The authorization code flow uses PKCE, browser-bound single-use state,
and nonce verification. The OIDC library checks the ID token signature, issuer,
audience, and expiry. Discovery/signing keys are refreshed during callbacks.
Rayflow stores neither access nor refresh tokens. Use a company-specific issuer
(for example, a specific Entra tenant or Keycloak realm) and restrict application
access at the provider. SAML systems need an OIDC bridge; direct SAML is not
implemented.

## LDAP / Active Directory

Add `auth.ldap` alongside or instead of `auth.oidc`:

```yaml
auth:
  enabled: true
  company_name: Example Company
  public_url: https://rayflow.example.com
  ldap:
    url: ldaps://directory.example.com:636
    bind_dn: cn=rayflow-reader,ou=services,dc=example,dc=com
    bind_password: supply-through-environment
    base_dn: ou=people,dc=example,dc=com
    user_filter: '(&(objectClass=person)(uid={username}))'
    name_attribute: displayName
    email_attribute: mail
```

Set the search account password using `RAYFLOW_AUTH__LDAP__BIND_PASSWORD`.
The service account needs search access and permission to read the name/email
attributes. The filter requires exactly one `{username}`. Rayflow escapes its
value, requires exactly one matching entry, and binds as that entry to verify
the password. Empty passwords are rejected before binding. Authentication has a
ten-second deadline.

For Active Directory, a filter could be
`(&(objectClass=user)(sAMAccountName={username})(memberOf=CN=Rayflow Users,OU=Groups,DC=example,DC=com))`.
This checks direct group membership; customize it to your directory policy.
`ldap://` always requires StartTLS before any bind; `ldaps://` uses TLS immediately.
Certificate verification is always enabled. Install your company's CA in the
server's certificate trust store if needed. LDAP identities use the configured
URL and entry DN; renaming an entry changes its Rayflow identity.

## Local accounts

Provision accounts with Argon2id PHC hashes. There are no built-in accounts or
default passwords. Generate a hash without placing the password in a command
argument (Bash example):

```sh
read -r -s -p 'Password: ' rayflow_password
printf '%s' "$rayflow_password" | cargo run -q -p server --bin hash-password
unset rayflow_password
```

Add the resulting hash to private configuration:

```yaml
auth:
  enabled: true
  public_url: https://rayflow.example.com
  local_users:
    - username: alice
      name: Alice Example
      password_hash: '$argon2id$v=19$...'
```

Restart after changing accounts. Existing sessions last until logout, revocation,
or expiry; account/password/directory changes do not revoke previous sessions.
To revoke immediately, delete the user's `auth_session` rows by `user_id`.
`auth_user.profile` records provider, subject, name, and email. Audit columns use
the stable 64-character `auth_user.user_id`, derived from provider and subject.
Each `auth_*` table has a separate auto-incrementing `BIGINT UNSIGNED` primary
`id`. User IDs, session token hashes, and login state hashes remain unique.
Migrations 14–16 rename the former `auth_user.id` to `user_id`, assign numeric
IDs, and align the tables with InnoDB, utf8mb4, and `DATETIME(6)`. Existing
profiles, session references, credentials, and expiry timestamps are preserved.

## Agents and sessions

When authentication is enabled, agents need a separate shared fleet credential
with at least 32 random printable ASCII characters. Generate one with
`openssl rand -hex 32` and configure the same value on server and agents:

```sh
# API server
export RAYFLOW_AUTH__AGENT_TOKEN='your-random-fleet-token'
# Each execution agent
export RAYFLOW_AGENT_AUTH_TOKEN='your-random-fleet-token'
```

This bearer token permits polling, job actions, catalog prepare/commit/download,
published graph reads, and individual run-state reads for input resolution. It
cannot edit drafts, publish, read management lists, start runs, or emit arbitrary
task events. Agents sharing the fleet token are mutually trusted; it does not
identify individual agents. If omitted, only signed-in users can access the API.
Use HTTPS for agent traffic outside loopback. `/health` and the root stay public.

The browser stores no credentials in local storage. Only session token digests
are stored in MySQL; login and logout work across replicas. Sessions have a fixed
lifetime (default eight hours, configurable from one minute to seven days).
Expired sessions and abandoned SSO flows are cleaned up during login activity.
Sign-out ends the Rayflow session, not the company identity provider's session.

| Server endpoint | Behavior |
| --- | --- |
| `GET /auth/providers` | Public company name and login choices, without secrets |
| `POST /auth/login` | JSON `provider` (`local`/`ldap`), `username`, `password` |
| `GET /auth/oidc/login` | Redirect to company SSO |
| `GET /auth/oidc/callback` | Consume state and complete SSO |
| `GET /auth/me` | Current user, or HTTP 401 |
| `POST /auth/logout` | Revoke the session and expire its cookie |

Cookie-authenticated writes, password login, and logout require
`X-Rayflow-Request: 1`. If present, `Origin` must exactly match `public_url`.
The console sends the header automatically; cross-origin CORS is not enabled.
Graph edits and publishing use the authenticated audit identity even when a
different `actor` is submitted.

Password login allows ten attempts per provider/username per minute and eight
concurrent verifications per process. SSO starts allow ten per minute per process.
Use deployment-level rate limits when scaling across replicas.

## Custom adapters

Implement `server::auth::PasswordProvider` (`id`, `label`, `authenticate`) for a
company-specific credential service. Return
`User::new(provider_namespace, stable_subject, name, email)` only after
verification. Pass adapters to `Auth::with_providers` and attach it using
`App::with_auth`. Built-ins can coexist with custom adapters. The standard binary
initializes built-ins automatically. Enabled routers without initialized
authentication fail closed with HTTP 503.

Protocol references: [OIDC library](https://docs.rs/openidconnect/4.1.0/openidconnect/),
[LDAP TLS settings](https://docs.rs/ldap3/0.12.1/ldap3/struct.LdapConnSettings.html),
[Argon2 hashing](https://docs.rs/argon2/0.5.3/argon2/).

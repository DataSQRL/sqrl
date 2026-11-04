# Vert.x Engine Configuration

Eclipse Vert.x is a reactive server framework that serves as the GraphQL API server, routing queries to the backing database/log engines.

## Configuration Options

| Key        | Type      | Default | Notes                                                    |
|------------|-----------|---------|----------------------------------------------------------|
| `authKind` | **array** | `[]`    | List of auth methods: `"JWT"`, `"OAUTH"`, or both        |
| `config`   | **object**| see below| Vert.x-specific configuration including auth settings   |

## Basic Configuration

```json
{
  "engines": {
    "vertx": {
      "authKind": []
    }
  }
}
```

## Server Configuration Overrides

The compiler generates the full server configuration from a built-in template and deep-merges `engines.vertx.config` over it, so any server setting can be overridden, not just authentication. Nested objects are merged key by key, while arrays replace the default array. Commonly overridden settings:

| Key | Notes |
|-----|-------|
| `httpServerOptions.port` | HTTP port (default `8888`). Keep `8888` for the `test` command, whose test runner connects to that port |
| `corsHandlerOptions` | CORS policy. Defaults allow all origins; restrict `allowedOrigin`/`allowedOrigins` in production |
| `poolOptions.maxSize` | PostgreSQL connection pool size |
| `servletConfig` | Endpoint paths (`graphQLEndpoint`, `restEndpoint`, `mcpEndpoint`, `graphiQLEndpoint`), prefixed with the API version, e.g. `/v1/graphql` |
| `publicGraphQLEndpointEnabled` | Set to `false` to disable the public GraphQL endpoint; REST and MCP endpoints keep working |
| `onlyConfiguredGraphQLOperations` | Set to `true` to only accept the predefined API operations instead of arbitrary GraphQL queries |

```json
{
  "engines": {
    "vertx": {
      "config": {
        "corsHandlerOptions": {
          "allowedOrigin": "https://app.example.com",
          "allowCredentials": true
        },
        "poolOptions": {
          "maxSize": 16
        }
      }
    }
  }
}
```

For all settings and their defaults, see the [server configuration template](https://github.com/DataSQRL/sqrl/blob/main/sqrl-cli/src/main/resources/templates/server-config.json) or the generated `vertx-config.json` in your build output (`build/deploy/plan`), which contains the effective configuration with your overrides applied.

## Authentication Configuration

Configure authentication with the `auth` block. It lists the token issuers the server trusts. Each issuer has its own
signing keys and audiences, and a token is accepted only when its `iss`, its `aud` and its signature all match **the
same** issuer. A token signed by one issuer can never be accepted under another issuer's name or for another issuer's
audiences.

```json
{
  "engines": {
    "vertx": {
      "config": {
        "auth": {
          "issuers": [
            {
              "issuer": "https://login.example.com/",
              "audiences": ["https://app.example.com"]
            },
            {
              "issuer": "https://tenant.auth0.com/",
              "audiences": ["https://api.example.com/"]
            }
          ],
          "resource": "https://api.example.com/v1/mcp",
          "scopesSupported": ["mcp:tools", "mcp:resources"]
        }
      }
    }
  }
}
```

The server creates one JWT validator per issuer and accepts a request when any of them accepts its bearer token. For
MCP clients it publishes the issuers as `authorization_servers` at `/.well-known/oauth-protected-resource` (see
[OAuth Discovery Endpoint](#oauth-discovery-endpoint)).

### `auth` Reference

| Key               | Type        | Required | Default                        | Description                                                                                         |
|-------------------|-------------|----------|--------------------------------|-----------------------------------------------------------------------------------------------------|
| `issuers`         | **array**   | Yes      |                                | The trusted issuers, see below. Must not be empty                                                   |
| `leeway`          | **integer** | No       | `60`                           | Allowed clock skew in seconds for `exp`, `nbf` and `iat`, for every issuer that does not set its own |
| `algorithms`      | **array**   | No       | `["RS256"]`                    | Accepted signature algorithms, for every issuer that does not set its own                           |
| `resource`        | **string**  | No       | derived from the request       | Public URL of the MCP endpoint (RFC 9728), e.g. `https://api.example.com/v1/mcp`                    |
| `scopesSupported` | **array**   | No       | `["mcp:tools", "mcp:resources"]` | Scopes advertised in the protected resource metadata                                              |

Each entry of `issuers`:

| Key          | Type        | Required | Description                                                                                                  |
|--------------|-------------|----------|--------------------------------------------------------------------------------------------------------------|
| `issuer`     | **string**  | Yes      | Must equal the token's `iss` claim exactly, including any trailing slash. Must be unique across `issuers`    |
| `audiences`  | **array**   | Yes      | The token's `aud` claim must contain at least one of them. Must not be empty                                 |
| `keys`       | **array**   | No       | Inline signing keys as JWK objects (RFC 7517)                                                                |
| `jwksUri`    | **string**  | No       | URL of a JWK Set to fetch the signing keys from                                                              |
| `leeway`     | **integer** | No       | Overrides `auth.leeway` for this issuer                                                                      |
| `algorithms` | **array**   | No       | Overrides `auth.algorithms` for this issuer                                                                  |

The effective leeway of an issuer is its own `leeway`, else `auth.leeway`, else 60 seconds. Negative values are rejected.

Set at most one of `keys` and `jwksUri`. With neither, the server uses OpenID Connect discovery: it fetches
`<issuer>/.well-known/openid-configuration` (a trailing slash on the issuer is not doubled), checks that the document
names the same `issuer`, and fetches the keys from its `jwks_uri`. Keys are fetched once at startup, and the server does
not start if they cannot be fetched.

Prefer discovery or `jwksUri`: inline `keys` cannot rotate, so every key change needs a configuration change and a
redeploy. Keys fetched at startup are not refreshed while the server runs, so restart the server after the identity
provider rotates its signing keys.

A key is only used with the algorithms the issuer allows: a key whose `alg` is not listed is ignored, and a key without
`alg` is used with each listed algorithm that fits its type (`RSA` for `RS*`/`PS*`, `EC` for `ES*`, `oct` for `HS*`,
`OKP` for `EdDSA`). A token signed with any other algorithm is rejected, so for example an `HS256` token cannot be
accepted by an issuer that only allows `RS256`. Supported algorithms are `HS256`, `HS384`, `HS512`, `RS256`, `RS384`,
`RS512`, `PS256`, `PS384`, `PS512`, `ES256`, `ES384`, `ES512` and `EdDSA`.

The server rejects these configurations at compile time and at startup: a missing or empty `issuers`, an issuer
without `issuer` or `audiences`, an empty `audiences` or `keys`, both `keys` and `jwksUri`, a duplicate issuer, an
unsupported algorithm, a negative leeway, and any unknown key in `auth` or in an issuer entry.

For symmetric keys, for example in tests, use an `oct` JWK whose `k` is the base64url-encoded secret, and allow `HS256`:

```json
"auth": {
  "algorithms": ["HS256"],
  "issuers": [
    {
      "issuer": "my-test-issuer",
      "audiences": ["my-test-audience"],
      "keys": [{ "kty": "oct", "k": "<base64url-encoded signer secret>" }]
    }
  ]
}
```

### Auth0 with a Custom Domain

An Auth0 tenant with a custom domain issues tokens under two issuers: user logins through the custom domain, and
machine-to-machine (client credentials) tokens through the tenant domain. Trust each one with the audiences it serves,
and let both discover their keys:

```json
"auth": {
  "issuers": [
    {
      "issuer": "https://login.example.com/",
      "audiences": ["https://app.example.com", "https://admin.example.com"]
    },
    {
      "issuer": "https://<your-tenant>.us.auth0.com/",
      "audiences": ["https://api.example.com/"]
    }
  ]
}
```

Auth0 issuers end with a trailing slash; keep it, because `issuer` must match the `iss` claim exactly.

### Layered Package Files

Package files merge objects key by key, but an array in a later file replaces the earlier array whole. A later layer
can therefore change `auth.leeway` or `auth.algorithms` alone and keep the earlier `issuers`, but it cannot change one
field of one issuer: a layer that sets `auth.issuers` must list every issuer of that environment.

`auth` cannot be combined with the deprecated `jwtAuth` or `oauthConfig`, including when they come from different
layers. If a shared layer still sets one of them, set it to `null` in the layer that introduces `auth`:

```json
"config": {
  "oauthConfig": null,
  "auth": { "issuers": [ ... ] }
}
```

### Migrating from `jwtAuth` and `oauthConfig`

`jwtAuth` and `oauthConfig` still work but are deprecated, and the server logs a warning when either is set.
`oauthConfig` checks neither the `iss` nor the `aud` claim, and with both blocks set a token that either one accepts is
accepted. Move to `auth`, where every trusted issuer is listed with its audiences.

`jwtAuth` with a shared secret:

```json
"jwtAuth": {
  "pubSecKeys": [{ "algorithm": "HS256", "buffer": "<base64 secret>" }],
  "jwtOptions": { "issuer": "my-issuer", "audience": ["my-api"], "leeway": 30 }
}
```

becomes (note that `k` is base64url, not base64):

```json
"auth": {
  "algorithms": ["HS256"],
  "leeway": 30,
  "issuers": [
    {
      "issuer": "my-issuer",
      "audiences": ["my-api"],
      "keys": [{ "kty": "oct", "k": "<base64url secret>" }]
    }
  ]
}
```

`oauthConfig` with OIDC discovery:

```json
"oauthConfig": {
  "oauth2Options": { "site": "https://<your-tenant>.auth0.com/" },
  "authorizationServerUrl": "https://<your-tenant>.auth0.com/",
  "resource": "https://api.example.com/v1/mcp"
}
```

becomes the following, where `audiences` is the API identifier your tokens are issued for:

```json
"auth": {
  "issuers": [
    { "issuer": "https://<your-tenant>.auth0.com/", "audiences": ["https://api.example.com/"] }
  ],
  "resource": "https://api.example.com/v1/mcp"
}
```

`authorizationServerUrl` has no equivalent: the authorization servers advertised to MCP clients are the issuers. If the
server cannot reach the issuer URL itself (for example inside a Docker network), set `jwksUri` to a URL it can reach. A
configuration with both `jwtAuth` and `oauthConfig` becomes one `auth` block with one entry per issuer.

## JWT Authentication Configuration (deprecated)

:::warning Deprecated
`jwtAuth` is deprecated. Use [`auth`](#authentication-configuration) instead, see [Migrating from `jwtAuth` and `oauthConfig`](#migrating-from-jwtauth-and-oauthconfig).
:::

For secure APIs with JWT authentication:

```json
{
  "engines": {
    "vertx": {
      "authKind": ["JWT"],
      "config": {
        "jwtAuth": {
          "pubSecKeys": [
            {
              "algorithm": "HS256",
              "buffer": "<signer-secret>" // Base64 encoded signer secret string
            }
          ],
          "jwtOptions": {
            "issuer": "<jwt-issuer>",
            "audience": ["<jwt-audience>"],
            "expiresInSeconds": 3600,
            "leeway": 30
          }
        }
      }
    }
  }
}
```

As these config fields will be mapped to Vert.x Java POJOs, the name of the key fields are very important.
For `pubSecKeys`, it is also possible to use different algorithms, that requires the key in a different (mostly PEM) format.
For example, for `ES256`, this would look something like this:
```json
{
  "pubSecKeys": [
    {
      "algorithm": "ES256",
      "buffer": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhk...restOfBase64...\n-----END PUBLIC KEY-----"
    }
  ]
}
```

## OAuth 2.0 Authentication Configuration (deprecated)

:::warning Deprecated
`oauthConfig` is deprecated: it checks neither the `iss` nor the `aud` claim of a token. Use [`auth`](#authentication-configuration) instead, see [Migrating from `jwtAuth` and `oauthConfig`](#migrating-from-jwtauth-and-oauthconfig).
:::

For OAuth 2.0 authentication with providers like Auth0 or Keycloak, use the `oauthConfig` section.
This enables MCP (Model Context Protocol) clients like Claude Code to authenticate using OAuth.

```json
{
  "engines": {
    "vertx": {
      "authKind": ["OAUTH"],
      "config": {
        "oauthConfig": {
          "oauth2Options": {
            "site": "https://your-tenant.auth0.com/",
            "clientId": "your-client-id"
          },
          "authorizationServerUrl": "https://your-tenant.auth0.com/",
          "scopesSupported": ["mcp:tools", "mcp:resources"]
        }
      }
    }
  }
}
```

### Combined JWT and OAuth Authentication

You can enable both deprecated authentication methods simultaneously. A token that either one accepts is accepted, so
prefer one `auth` block that lists every issuer with its audiences:

```json
{
  "engines": {
    "vertx": {
      "authKind": ["JWT", "OAUTH"],
      "config": {
        "jwtAuth": { ... },
        "oauthConfig": { ... }
      }
    }
  }
}
```

### OAuthConfig Structure

The `oauthConfig` object combines Vert.x's [OAuth2Options](https://vertx.io/docs/apidocs/io/vertx/ext/auth/oauth2/OAuth2Options.html) with discovery metadata:

| Key                      | Type       | Required | Description                                                           |
|--------------------------|------------|----------|-----------------------------------------------------------------------|
| `oauth2Options`          | **object** | Yes      | Vert.x OAuth2Options for authentication                               |
| `authorizationServerUrl` | **string** | No       | External authorization server URL for discovery                       |
| `scopesSupported`        | **array**  | No       | Scopes advertised (default: `["mcp:tools", "mcp:resources"]`)         |
| `resource`               | **string** | No       | Public URL of the MCP endpoint, e.g. `https://api.example.com/v1/mcp` |

### OAuth2Options Configuration

The `oauth2Options` object uses Vert.x's OAuth2Options class:

| Key        | Type       | Required | Description                                           |
|------------|------------|----------|-------------------------------------------------------|
| `site`     | **string** | Yes      | OAuth issuer URL (e.g., `https://tenant.auth0.com`)   |
| `clientId` | **string** | No       | OAuth client ID (defaults to `datasqrl-mcp`)          |

### OAuth Discovery Endpoint

When `auth` (or the deprecated `oauthConfig`) is configured, the server exposes a discovery endpoint at `/.well-known/oauth-protected-resource` per RFC 9728.
This enables MCP clients to discover the authorization server and scopes. With `auth`, the advertised `authorization_servers` are the configured issuers.
When an MCP request has no valid token, the server responds with `401` and a `WWW-Authenticate: Bearer resource_metadata="..."` header that points MCP clients to this endpoint.

If `resource` is not set, the server derives both the `resource` value in the discovery metadata and the `resource_metadata` URL from each request: the scheme from the `X-Forwarded-Proto` header (or the request scheme), and the host from the `Host` header.
If `resource` is set, the discovery metadata returns it unchanged, and the `resource_metadata` URL uses its origin.
For example, `"resource": "https://api.example.com/v1/mcp"` makes the `401` header point to `https://api.example.com/.well-known/oauth-protected-resource`.

Set `resource` when the server runs behind a reverse proxy or load balancer that terminates TLS, so MCP clients always receive `https` URLs.
Use the exact URL MCP clients connect to: RFC 9728 requires clients to reject discovery metadata whose `resource` does not match that URL.
The proxy must also route `/.well-known/oauth-protected-resource` on the same origin to the server.

### Environment Variable Support

You can use environment variables in the OAuth configuration:

```json
{
  "oauthConfig": {
    "oauth2Options": {
      "site": "${AUTH0_ISSUER}"
    },
    "authorizationServerUrl": "${AUTH0_EXTERNAL_URL}"
  }
}
```

### OAuth 2.0 with Auth0

[Auth0](https://auth0.com) is a managed identity platform that works as a drop-in OAuth 2.0 / OIDC provider for DataSQRL.
Because Auth0 is a public cloud service, the issuer URL is reachable from both inside your Docker/Kubernetes network and from MCP clients—no internal vs. external URL distinction is needed.

#### Auth0 Setup

**1. Create an API (Resource Server)**

In the Auth0 dashboard go to **Applications → APIs → Create API** and fill in:

| Field       | Value                                               |
|-------------|-----------------------------------------------------|
| Name        | A descriptive name, e.g. `My MCP Server`            |
| Identifier  | The audience URI, e.g. `https://my-mcp-server/`     |

Enable **RBAC** if you want scope-based access control, then add custom scopes such as `mcp:tools` and `mcp:resources` in the **Permissions** tab.

**2. Create a Machine-to-Machine Application**

Go to **Applications → Applications → Create Application**, choose **Machine to Machine Applications**, and authorize it against the API you just created.
Grant the scopes that MCP clients should receive.

Copy the **Client ID** and **Client Secret** — you will need them to request tokens.

**3. (Optional) Create a Regular Web App for user-facing auth**

If MCP clients authenticate on behalf of users, create a **Regular Web Application** instead and register your callback URL under **Allowed Callback URLs**.

#### DataSQRL Configuration

:::tip
Prefer the [`auth`](#authentication-configuration) block, which also checks the issuer and audience of every token; see [Auth0 with a Custom Domain](#auth0-with-a-custom-domain). The rest of this section describes the deprecated `oauthConfig`.
:::

Set `site` and `authorizationServerUrl` to your Auth0 tenant URL.

:::warning
The Auth0 tenant URL ends with a trailing slash, and Auth0 uses it with the slash as the issuer identifier. Keep the slash in `authorizationServerUrl` so the authorization server advertised to MCP clients matches Auth0's issuer exactly. For `site`, the slash is optional: the server removes it before fetching the OIDC discovery document.
:::

```json
{
  "engines": {
    "vertx": {
      "authKind": ["OAUTH"],
      "config": {
        "oauthConfig": {
          "oauth2Options": {
            "site": "https://<your-tenant>.auth0.com/"
          },
          "authorizationServerUrl": "https://<your-tenant>.auth0.com/",
          "scopesSupported": ["mcp:tools", "mcp:resources"]
        }
      }
    }
  }
}
```

Using environment variables (recommended so credentials stay out of source control):

```json
{
  "engines": {
    "vertx": {
      "authKind": ["OAUTH"],
      "config": {
        "oauthConfig": {
          "oauth2Options": {
            "site": "${AUTH0_ISSUER}"
          },
          "authorizationServerUrl": "${AUTH0_EXTERNAL_URL}"
        }
      }
    }
  }
}
```

Then pass the variables at compile and server startup time:

```bash
AUTH0_ISSUER=https://<your-tenant>.auth0.com/
AUTH0_EXTERNAL_URL=https://<your-tenant>.auth0.com/
```

Because Auth0 is a public cloud service, both variables point to the same URL.

#### Obtaining a Token (Client Credentials / M2M)

For server-to-server access use the **client credentials** grant:

```bash
curl -s -X POST https://<your-tenant>.auth0.com/oauth/token \
  -H "Content-Type: application/json" \
  -d '{
    "grant_type":    "client_credentials",
    "client_id":     "<CLIENT_ID>",
    "client_secret": "<CLIENT_SECRET>",
    "audience":      "https://my-mcp-server/"
  }'
```

The `audience` field is **required** for Auth0 client credentials requests. Without it, Auth0 returns an opaque token rather than a JWT, which the DataSQRL server cannot validate.

#### Auth0-specific Notes

- **Trailing slash on issuer** — Auth0's issuer is always `https://<tenant>.auth0.com/` (with the slash). Use that exact value for `authorizationServerUrl`.
- **Issuer and audience claims** — With `oauthConfig`, the server only verifies the token signature against the tenant's signing keys; it does not check the `iss` or `aud` claims. Any valid token issued by the tenant is accepted, including tokens issued for other APIs of the same tenant.
- **JWKS endpoint** — Auth0 publishes signing keys at `https://<tenant>.auth0.com/.well-known/jwks.json`. The server discovers them from the OIDC discovery document (`/.well-known/openid-configuration`) and fetches them once at startup. Restart the server after Auth0 rotates its signing keys.
- **Custom domains** — If your Auth0 tenant uses a custom domain (e.g. `https://auth.example.com/`), use that URL as both `site` and `authorizationServerUrl`. Tokens issued through the custom domain and through the tenant domain carry different `iss` values; to accept both, use `auth` with one issuer entry each.

## Cloud Deployment

For cloud deployment configuration (instance sizes, instance counts), see [Cloud Deployment Configuration](cloud-deployment.md#vertx-enginesvertxdeployment).

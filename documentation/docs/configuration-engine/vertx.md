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

## JWT Authentication Configuration

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

## OAuth 2.0 Authentication Configuration

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

You can enable both authentication methods simultaneously:

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

| Key                      | Type       | Required | Description                                                   |
|--------------------------|------------|----------|---------------------------------------------------------------|
| `oauth2Options`          | **object** | Yes      | Vert.x OAuth2Options for authentication                       |
| `authorizationServerUrl` | **string** | No       | External authorization server URL for discovery               |
| `scopesSupported`        | **array**  | No       | Scopes advertised (default: `["mcp:tools", "mcp:resources"]`) |
| `resource`               | **string** | No       | Override resource identifier in discovery metadata            |

### OAuth2Options Configuration

The `oauth2Options` object uses Vert.x's OAuth2Options class:

| Key        | Type       | Required | Description                                           |
|------------|------------|----------|-------------------------------------------------------|
| `site`     | **string** | Yes      | OAuth issuer URL (e.g., `https://tenant.auth0.com`)   |
| `clientId` | **string** | No       | OAuth client ID (defaults to `datasqrl-mcp`)          |

### OAuth Discovery Endpoint

When OAuth is configured, the server exposes a discovery endpoint at `/.well-known/oauth-protected-resource` per RFC 9728.
This enables MCP clients to discover the authorization server and scopes.

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
- **Custom domains** — If your Auth0 tenant uses a custom domain (e.g. `https://auth.example.com/`), use that URL as both `site` and `authorizationServerUrl`.

## Cloud Deployment

For cloud deployment configuration (instance sizes, instance counts), see [Cloud Deployment Configuration](cloud-deployment.md#vertx-enginesvertxdeployment).

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

## JWT Authentication Configuration

For secure APIs with JWT authentication:

```json5
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
For `pubSecKeys`, it is also possible to use different algorithms, thet requires the key in a different (mostly PEM) format.
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
The tenant URL always ends with a trailing slash. This must match exactly because Auth0 includes the slash in the `iss` claim of every JWT it issues.
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

- **Trailing slash on issuer** — Auth0's issuer is always `https://<tenant>.auth0.com/` (with the slash). The value in `site` must match the `iss` claim in Auth0 JWTs exactly; a missing slash causes validation failures.
- **Audience claim** — Auth0 JWTs include an `aud` claim set to the API identifier you configured. The DataSQRL server validates this automatically via OIDC discovery.
- **JWKS endpoint** — Auth0 publishes signing keys at `https://<tenant>.auth0.com/.well-known/jwks.json`. The server fetches these automatically from the OIDC discovery document (`/.well-known/openid-configuration`) and rotates them without restart.
- **Custom domains** — If your Auth0 tenant uses a custom domain (e.g. `https://auth.example.com/`), use that URL as both `site` and `authorizationServerUrl`.

## Deployment Configuration

Vert.x supports deployment-specific configuration options for scaling the API server:

| Key              | Type        | Default | Description                                    |
|------------------|-------------|---------|------------------------------------------------|
| `instance-size`  | **string**  | -       | Server instance size with storage variants     |
| `instance-count` | **integer** | -       | Number of server instances to run (minimum: 1) |

### Instance Size Options

Available `instance-size` options with storage variants:
- `dev` - Development/testing size
- `small`, `small.disk` - Small instances with optional additional disk
- `medium`, `medium.disk` - Medium instances with optional additional disk
- `large`, `large.disk` - Large instances with optional additional disk

### Deployment Example

```json
{
  "engines": {
    "vertx": {
      "deployment": {
        "instance-size": "medium.disk",
        "instance-count": 3
      }
    }
  }
}
```

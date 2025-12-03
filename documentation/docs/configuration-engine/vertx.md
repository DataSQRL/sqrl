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

| Key                      | Type       | Required | Description                                              |
|--------------------------|------------|----------|----------------------------------------------------------|
| `oauth2Options`          | **object** | Yes      | Vert.x OAuth2Options for authentication                  |
| `authorizationServerUrl` | **string** | No       | External authorization server URL for discovery          |
| `scopesSupported`        | **array**  | No       | Scopes advertised (default: `["mcp:tools", "mcp:resources"]`) |
| `resource`               | **string** | No       | Override resource identifier in discovery metadata       |

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

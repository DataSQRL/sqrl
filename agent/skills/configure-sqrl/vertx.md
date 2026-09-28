# Vert.x Engine Configuration

| Key        | Type       | Default   | Notes                                                 |
|------------|------------|-----------|-------------------------------------------------------|
| `authKind` | **array**  | `[]`      | List of auth methods: `"JWT"`, `"OAUTH"`, or both     |
| `config`   | **object** | see below | Vert.x-specific configuration including auth settings |

## JWT Authentication

Set `authKind` to include `JWT` and provide a `jwtAuth` object. Its field names map to Vert.x Java POJOs and are case-sensitive.

```json
{
  "engines": {
    "vertx": {
      "authKind": ["JWT"],
      "config": {
        "jwtAuth": {
          "pubSecKeys": [{
            "algorithm": "HS256",
            "buffer": "<base64-encoded signer secret>"
          }],
          "jwtOptions": {
            "issuer": "https://issuer.example",
            "audience": ["my-api"],
            "expiresInSeconds": 3600,
            "leeway": 30
          }
        }
      }
    }
  }
}
```

For asymmetric algorithms such as `ES256`, `buffer` must contain the appropriately formatted public key (normally PEM).

## OAuth 2.0 Authentication

Set `authKind` to include `OAUTH` and configure `oauthConfig`. OAuth enables MCP clients to discover the authorization server at `/.well-known/oauth-protected-resource`.

```json
{
  "engines": {
    "vertx": {
      "authKind": ["OAUTH"],
      "config": {
        "oauthConfig": {
          "oauth2Options": {
            "site": "${AUTH_ISSUER}",
            "clientId": "my-client-id"
          },
          "authorizationServerUrl": "${AUTH_EXTERNAL_URL}",
          "scopesSupported": ["mcp:tools", "mcp:resources"],
          "resource": "https://api.example"
        }
      }
    }
  }
}
```

`oauth2Options.site` is required. `clientId` defaults to `datasqrl-mcp`; `authorizationServerUrl`, `scopesSupported`, and `resource` are optional. Use `["JWT", "OAUTH"]` and provide both configuration blocks to enable both methods.

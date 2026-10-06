# Vert.x Engine Configuration

| Key        | Type       | Default   | Notes                                                 |
|------------|------------|-----------|-------------------------------------------------------|
| `authKind` | **array**  | `[]`      | List of auth methods: `"JWT"`, `"OAUTH"`, or both     |
| `config`   | **object** | see below | Vert.x-specific configuration including auth settings |

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

For all settings and their effective values, compile the project and read the generated `deploy/plan/vertx-config.json` under the build directory (`build/` normally or `build/<sub-project>/` when compiling with `-b`). It contains the full server configuration with the overrides from `engines.vertx.config` applied.

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

# Vert.x Engine Configuration

| Key        | Type       | Default   | Notes                                                 |
|------------|------------|-----------|-------------------------------------------------------|
| `authKind` | **array**  | `[]`      | List of auth methods: `"JWT"`, `"OAUTH"`, or both. It does not turn on a method: `config.jwtAuth` or `config.oauthConfig` does |
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

## Slow-Query Logging

Enable slow-query logging in every project that enables `vertx`. The server then logs each API request that takes longer than its threshold or that fails. A request's threshold is the lowest threshold among the fields it runs. A slow query then appears in the logs, not only as a client timeout.

Put the block in the base config, so every deployed environment has it:

```json
{
  "engines": {
    "vertx": {
      "config": {
        "graphQLTailSampleTracingConfig": {
          "enabled": true,
          "defaultThresholdMs": 2000
        }
      }
    }
  }
}
```

All keys go under `engines.vertx.config.graphQLTailSampleTracingConfig`:

| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `false` | Turns the logging on |
| `defaultThresholdMs` | `60000` | The threshold for fields without an entry in `thresholds` |
| `thresholds` | `{}` | A threshold for requests that run this field, keyed `Type.field`, e.g. `{"Query.OrdersByCustomer": 5000}` or `{"Customers.orders": 5000}`. A request uses the lowest threshold among the fields it runs |
| `alwaysLogErrors` | `true` | Also logs failed requests, such as a timeout while waiting for a database connection |
| `maxLoggedTracesPerMinute` | `60` | Limits the log volume during an incident |
| `maxFieldTraces` | `50` | Maximum number of query timings in one log line |

### Choosing the threshold

Make an educated guess for each threshold from how the API is used. The values in the example are a starting point, not a default to copy.

- If the requirements state a latency target for the API, use that target. Otherwise, start from the consumer of the API: 1000 to 2000 ms for user interfaces and dashboards, 2000 to 5000 ms for service-to-service calls.
- Look for heavy queries, such as a large aggregate. If their latency is much higher than that of the other queries, give each one its own entry in `thresholds`, keyed by its field (`Query.<Field>` for a table or table function, `<Type>.<field>` for a relationship). As a request uses the lowest threshold among its fields, the entry applies only when a request runs that field alone. Any other field in the same request, including a nested relationship field without its own entry, brings the lower threshold back.

- Keep every threshold well below the client request timeout. The server writes the log line after a request ends, and it does not stop the request. A threshold below the timeout logs the requests that are slow but still succeed.

## JWT Authentication

To turn on the JWT check, set `engines.vertx.config.jwtAuth`, and include `JWT` in `authKind`. The `jwtAuth` field names map to Vert.x Java POJOs and are case-sensitive.
With the check on, every request needs a valid token.

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

To turn on the OAuth check, set `engines.vertx.config.oauthConfig.oauth2Options.site`, and include `OAUTH` in `authKind`. OAuth enables MCP clients to discover the authorization server at `/.well-known/oauth-protected-resource`.

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
          "resource": "https://api.example.com/v1/mcp"
        }
      }
    }
  }
}
```

`oauth2Options.site` is required. `clientId` defaults to `datasqrl-mcp`; `authorizationServerUrl`, `scopesSupported`, and `resource` are optional. Use `["JWT", "OAUTH"]` and provide both configuration blocks to enable both methods.

`resource` is the public URL of the MCP endpoint. Without it, the server derives the discovery `resource` and the `WWW-Authenticate: Bearer resource_metadata="..."` URL of `401` responses from each request (`X-Forwarded-Proto` or the request scheme, plus `Host`). With it, the discovery metadata returns it unchanged, and `resource_metadata` points to `/.well-known/oauth-protected-resource` on its origin. Set it behind a proxy that terminates TLS, so MCP clients always receive `https` URLs.

## JWT Authentication Configuration

For secure APIs with JWT authentication with Vert.x server configuration:

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

The `oauthConfig` object combines Vert.x's OAuth2Options with discovery metadata:

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

## Testing Authorization

You can test record filtering, data masking, and other types of authorization based data access control with DataSQRL's automated test runner via the [`test` command](../compiler#test-command).

### Generating Tokens

Generate test tokens with [jwt.io](https://jwt.io/) or another JWT tool. For
testing, the `HS256` (shared-secret) algorithm is simplest and is what these
examples use. Do not assume a `jwt` command is available in the agent image.

A single secret string is the signing key, and it relates to the config two ways:

- The signing key configured in the JWT tool is the **raw** secret string.
- **`buffer`** in the `vertx` config is that same secret, **Base64-encoded**
  (Vert.x Base64-decodes `buffer` back to the raw key). Encode it with `printf`
  — not `echo`, which appends a trailing newline that changes the key and makes
  every token fail to validate:

```sh
printf '%s' 'mySuperSecretSignerStringThatIsLongEnough' | base64
# -> bXlTdXBlclNlY3JldFNpZ25lclN0cmluZ1RoYXRJc0xvbmdFbm91Z2g=
```

Set that encoded value as the `buffer` in the `package.json` `vertx` config section:

```json
{
  ...
  "engines" : {
    "vertx" : {
      "authKind": ["JWT"],
      "config": {
        "jwtAuth": {
          "pubSecKeys": [
            {
              "algorithm": "HS256",
              "buffer": "bXlTdXBlclNlY3JldFNpZ25lclN0cmluZ1RoYXRJc0xvbmdFbm91Z2g="
            }
          ],
          ...
        }
      }
    }
  },
  ...
}
```

Then mint a token with the raw secret and claims shaped to the test's needs;
`iss` and `aud` must match the `jwtOptions` configured above. In jwt.io, select
`HS256`, enter the raw secret as the signing key, and use a payload such as:

```json
{
  "iss": "<jwt-issuer>",
  "aud": ["<jwt-audience>"],
  "exp": 9999999999,
  "customerId": 6,
  "roles": ["user"]
}
```

Use the same tool to inspect a token and verify its signature with the raw secret.

### Default Test Runner Token

We can set one token directly to the `test-runner` configuration that the deployed test server will pick up by default.
Any valid HTTP headers can be defined in `headers` if necessary, but in this context the important one is `Authorization`.
These headers will be added to any request that will be executed during the test.

```json
{
  "test-runner": {
    "headers": {
      "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJteS10ZXN0LWlzc3VlciIsImF1ZCI6WyJteS10ZXN0LWF1ZGllbmNlIl0sImV4cCI6OTk5OTk5OTk5OSwidmFsIjoxfQ.cvgte5Lfhrsr2OPoRM9ecJbxehBQzwHaghANY6MvhqE"
    }
  }
}
```

### Test Specific Token

To be able to test different scenarios, it is mandatory to be able to provide different tokens that simulate them.
To achieve this, we can define any new test case under the project's `test-folder`, the test execution will pick them up and also compare it with their respective snapshots.
A custom JWT test case will require two files, which share the same name that will function as the name of the test case:
* A `.graphql` file that should define a query, mutation, or subscription.
* A `.properties` file if the test case requires a different token than the one defined in `test-runner

A sample test case structure with three different test cases looks like the below file tree.

```
├── tests/
│   ├── mutationWithSameToken.graphql
│   ├── subscriptionWithSameToken.graphql
│   ├── differentUserQuery.graphql
│   ├── differentUserQuery.properties
│   ...
```

The content of the `.properties` override the applied `headers` tor the matching `.graphql` requests, making it possible to define different scenarios.
A simple JWT override header properties file would look like this:

```properties
Authorization: Bearer <test-specific-jwt>
```

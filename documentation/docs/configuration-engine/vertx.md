# Vert.x Engine Configuration

Eclipse Vert.x is a reactive server framework that serves as the GraphQL API server, routing queries to the backing database/log engines.

## Configuration Options

| Key        | Type       | Default   | Notes                                                |
|------------|------------|-----------|------------------------------------------------------|
| `authKind` | **string** | `"NONE"`  | Authentication type: `"NONE"` or `"JWT"`             |
| `config`   | **object** | see below | Vert.x-specific configuration including JWT settings |

## Basic Configuration

```json
{
  "engines": {
    "vertx": {
      "authKind": "NONE"
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
      "authKind": "JWT",
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

## Usage Notes

- No mandatory keys required for basic operation
- Connection pools to databases are generated automatically from the overall plan  
- JWT authentication provides secure access to your GraphQL API
- The server exposes GraphQL, REST, and MCP endpoints based on compiler configuration
- Deployment configuration allows horizontal scaling for high-availability setups
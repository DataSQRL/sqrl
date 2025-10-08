# Vert.x Engine Configuration

Eclipse Vert.x is a reactive server framework that serves as the GraphQL API server, routing queries to the backing database/log engines.

## Configuration Options

| Key          | Type       | Default   | Notes                     |
|--------------|------------|-----------|---------------------------|
| `authKind`   | **string** | `"NONE"`  | Authentication type: `"NONE"` or `"JWT"` |
| `config`     | **object** | see below | Vert.x-specific configuration including JWT settings |

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
              "buffer": "<signer-secret>"   // Base64 encoded signer secret string
            }
          ],
          "jwtOptions": {
            "issuer": "<jwt-issuer>",
            "audience": ["<jwt-audience>"],
            "expiresInSeconds": "3600",
            "leeway": "60"
          }
        }
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
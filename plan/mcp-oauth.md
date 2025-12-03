# OAuth 2.0 Authentication for MCP Server

## Overview

Add OAuth 2.0 authentication support to the DataSQRL MCP server for Claude Code compatibility, using Auth0 as the OAuth provider.

## Problem

Claude Code expects MCP servers to implement OAuth 2.0 Protected Resource Metadata (RFC 9728). Currently getting 404 because `/.well-known/oauth-protected-resource` doesn't exist.

---

## Part 1: Package.json Schema Changes

### Current State
- `authKind` is a single enum: `["NONE", "JWT"]`
- Location: `sqrl-planner/src/main/resources/jsonSchema/packageSchema.json` (lines 242-245)

### New Design: `enabledAuth` Array

Replace single `authKind` with `enabledAuth` array to support multiple auth methods:

**Schema Change** in `packageSchema.json`:
```json
"vertx": {
  "type": "object",
  "properties": {
    "enabledAuth": {
      "type": "array",
      "items": {
        "type": "string",
        "enum": ["JWT", "OAUTH"]
      },
      "uniqueItems": true
    },
    "config": {
      "type": "object",
      "minProperties": 1
    }
  }
}
```

**Backward Compatibility**: Keep `authKind` as deprecated alias that maps to `enabledAuth`:
- `"authKind": "NONE"` → `"enabledAuth": []`
- `"authKind": "JWT"` → `"enabledAuth": ["JWT"]`

### Files to Modify

| File | Changes |
|------|---------|
| `sqrl-planner/src/main/resources/jsonSchema/packageSchema.json` | Replace authKind with enabledAuth array |
| `sqrl-planner/src/main/java/com/datasqrl/config/EngineConfigImpl.java` | Add helper for reading enabledAuth |

---

## Part 2: New Test Project `oauth-authorized`

Create new test case at: `sqrl-testing/sqrl-testing-integration/src/test/resources/usecases/oauth-authorized/`

### Files to Create

**package.json**:
```json
{
  "version": "1",
  "script": {
    "main": "oauth-authorized.sqrl"
  },
  "engines": {
    "vertx": {
      "enabledAuth": ["OAUTH"],
      "config": {
        "oauthConfig": {
          "provider": "keycloak",
          "issuer": "${KEYCLOAK_ISSUER}",
          "audience": "datasqrl-mcp",
          "scopesSupported": ["mcp:tools", "mcp:resources"],
          "useJwks": true
        }
      }
    }
  }
}
```

**oauth-authorized.sqrl**: Copy from jwt-authorized.sqrl or udf test case

---

## Implementation Steps

### 1. Add Maven Dependencies

**Root pom.xml** - Add to dependency management:
```xml
<dependency>
    <groupId>io.vertx</groupId>
    <artifactId>vertx-auth-oauth2</artifactId>
    <version>${vertx.version}</version>
</dependency>
```

**sqrl-server-vertx-base/pom.xml** - Add dependency:
```xml
<dependency>
    <groupId>io.vertx</groupId>
    <artifactId>vertx-auth-oauth2</artifactId>
</dependency>
```

### 2. Create OAuthConfig.java

**New file**: `sqrl-server/sqrl-server-vertx-base/src/main/java/com/datasqrl/graphql/config/OAuthConfig.java`

Fields:
- `provider` - OAuth provider type (auth0, keycloak, etc.)
- `issuer` - Auth0 tenant URL (e.g., https://your-tenant.auth0.com/)
- `audience` - Auth0 API identifier
- `scopesSupported` - List of scopes (default: mcp:tools, mcp:resources)
- `useJwks` - Whether to use JWKS for key rotation
- `jwksUri` - Optional JWKS URI override
- `resource` - Resource identifier for this server

### 3. Update ServerConfig.java

**File**: `sqrl-server/sqrl-server-vertx-base/src/main/java/com/datasqrl/graphql/config/ServerConfig.java`

Add field and setter for `OAuthConfig oauthConfig`

### 4. Create OAuthDiscoveryHandler.java

**New file**: `sqrl-server/sqrl-server-vertx-base/src/main/java/com/datasqrl/graphql/auth/OAuthDiscoveryHandler.java`

Registers `/.well-known/oauth-protected-resource` endpoint returning:
```json
{
  "resource": "https://example.com/v1/mcp",
  "authorization_servers": ["https://your-tenant.auth0.com/"],
  "scopes_supported": ["mcp:tools", "mcp:resources"],
  "bearer_methods_supported": ["header"]
}
```

### 5. Create OAuth2AuthFactory.java

**New file**: `sqrl-server/sqrl-server-vertx-base/src/main/java/com/datasqrl/graphql/auth/OAuth2AuthFactory.java`

Creates JWTAuth using JWKS from Auth0's `.well-known/jwks.json` endpoint for automatic key rotation.

### 6. Create OAuthFailureHandler.java

**New file**: `sqrl-server/sqrl-server-vertx-base/src/main/java/com/datasqrl/graphql/auth/OAuthFailureHandler.java`

Returns 401 with `WWW-Authenticate` header pointing to resource metadata:
```
WWW-Authenticate: Bearer resource_metadata="https://example.com/.well-known/oauth-protected-resource"
```

### 7. Update HttpServerVerticle.java

**File**: `sqrl-server/sqrl-server-vertx-base/src/main/java/com/datasqrl/graphql/HttpServerVerticle.java`

- Register OAuth discovery routes in `bootstrap()`
- Update `deployVersionedModel()` to use async auth provider creation
- Add `createAuthProvider()` method that prefers OAuth config, falls back to JWT config

### 8. Update McpBridgeVerticle.java

**File**: `sqrl-server/sqrl-server-vertx-base/src/main/java/com/datasqrl/graphql/McpBridgeVerticle.java`

Replace `JwtFailureHandler` with `OAuthFailureHandler` for both POST and SSE routes.

### 9. Add Unit Tests

**New file**: `sqrl-server/sqrl-server-vertx-base/src/test/java/com/datasqrl/graphql/auth/OAuthDiscoveryHandlerTest.java`

Test cases:
- `givenOAuthConfig_whenFetchProtectedResourceMetadata_thenReturnsValidJson`
- `givenNoOAuthConfig_whenFetchMetadata_thenReturns404`

### 10. Add Integration Tests

**File**: `sqrl-testing/sqrl-testing-container/src/test/java/com/datasqrl/container/testing/McpValidationIT.java`

Add tests:
- `givenOAuthConfig_whenMcpServerStarted_thenOAuthDiscoveryEndpointsWork`
- `givenOAuthConfig_whenUnauthorizedMcpRequest_thenReturns401WithWWWAuthenticate`

## Configuration Example

**vertx-config.json**:
```json
{
  "oauthConfig": {
    "provider": "auth0",
    "issuer": "https://your-tenant.auth0.com/",
    "audience": "https://your-api-identifier",
    "scopesSupported": ["mcp:tools", "mcp:resources"],
    "useJwks": true
  }
}
```

## Backward Compatibility

- **Legacy JWT config**: If only `jwtAuth` configured, uses HMAC/RSA JWT validation as before
- **No auth**: If neither configured, runs without authentication
- **OAuth mode**: If `oauthConfig` present, takes precedence and enables OAuth discovery

## Files Summary

### New Files
| File | Purpose |
|------|---------|
| `config/OAuthConfig.java` | OAuth configuration POJO |
| `auth/OAuthDiscoveryHandler.java` | RFC 9728 discovery endpoints |
| `auth/OAuth2AuthFactory.java` | Auth provider with JWKS |
| `auth/OAuthFailureHandler.java` | 401 handler with WWW-Authenticate |
| `auth/OAuthDiscoveryHandlerTest.java` | Unit tests |
| `testing/McpOAuthIT.java` | Keycloak integration tests |
| `usecases/oauth-authorized/*` | OAuth test project (package.json, sqrl file) |

### Modified Files
| File | Changes |
|------|---------|
| `/pom.xml` | Add vertx-auth-oauth2 dependency management |
| `sqrl-server-vertx-base/pom.xml` | Add vertx-auth-oauth2 dependency |
| `config/ServerConfig.java` | Add OAuthConfig field |
| `HttpServerVerticle.java` | Register discovery routes, async auth creation |
| `McpBridgeVerticle.java` | Use OAuthFailureHandler |
| `McpValidationIT.java` | Add OAuth integration tests |
| `packageSchema.json` | Replace authKind with enabledAuth array |
| `EngineConfigImpl.java` | Add helper for reading enabledAuth |
| `documentation/docs/configuration-engine/vertx.md` | Update auth documentation |

## Integration Testing with Keycloak Container

For automated integration tests, use Keycloak as a self-hosted OAuth provider in Docker.

### Test Infrastructure

**Keycloak Container Setup**:
- Use `quay.io/keycloak/keycloak:latest` image
- Configure realm, client, and user programmatically
- Generate tokens via Keycloak Admin API or direct grant

**Test Flow**:
1. Start Keycloak container with pre-configured realm
2. Start MCP server with oauthConfig pointing to Keycloak
3. Obtain access token from Keycloak (client credentials or direct grant)
4. Test MCP endpoints with Bearer token

### New Integration Test

**New file**: `sqrl-testing/sqrl-testing-container/src/test/java/com/datasqrl/container/testing/McpOAuthIT.java`

```java
@Testcontainers
class McpOAuthIT extends SqrlContainerTestBase {

    private static GenericContainer<?> keycloak;

    @BeforeAll
    static void startKeycloak() {
        keycloak = new GenericContainer<>("quay.io/keycloak/keycloak:latest")
            .withExposedPorts(8080)
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
            .withCommand("start-dev")
            .waitingFor(Wait.forHttp("/health/ready").forPort(8080));
        keycloak.start();

        // Configure realm, client via Admin API
    }

    @Test
    void givenKeycloakOAuth_whenMcpAuthenticated_thenToolCallSucceeds() {
        // 1. Get token from Keycloak
        // 2. Call MCP with Bearer token
        // 3. Verify response
    }
}
```

### Keycloak Realm Configuration

Create realm JSON or use Admin API to configure:
- Realm: `datasqrl`
- Client: `mcp-client` (public or confidential)
- User: `test-user` with password
- Scopes: `mcp:tools`, `mcp:resources`

## Manual Testing with Claude Code (Auth0)

For production/manual testing with Claude Code:
1. Configure Auth0 Application with API identifier
2. Update vertx-config.json with oauthConfig pointing to Auth0
3. Start MCP server
4. Connect Claude Code - OAuth flow should complete via browser
5. Verify tool calls work with valid token

/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.graphql.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * OAuth 2.0 configuration for MCP server authentication. Supports Auth0, Keycloak, and other OAuth
 * 2.0 / OpenID Connect providers.
 */
@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OAuthConfig {

  /** OAuth provider type (auth0, keycloak, etc.) - informational only */
  private String provider = "auth0";

  /** OAuth issuer URL (e.g., https://your-tenant.auth0.com/) */
  private String issuer;

  /** OAuth audience - the API identifier for token validation */
  private String audience;

  /** Client ID - used for token introspection if needed */
  private String clientId;

  /** Client secret - used for token introspection if needed */
  private String clientSecret;

  /** Scopes supported by this MCP server */
  private List<String> scopesSupported = List.of("mcp:tools", "mcp:resources");

  /** Whether to use JWKS for key rotation (recommended) */
  private boolean useJwks = true;

  /** JWKS URI override - defaults to issuer + .well-known/jwks.json */
  private String jwksUri;

  /** Resource identifier for this server (used in protected resource metadata) */
  private String resource;

  /**
   * External authorization server URL for discovery metadata. If set, this URL is returned in the
   * /.well-known/oauth-protected-resource endpoint instead of the issuer. Useful when the issuer
   * URL is only reachable internally (e.g., Docker network) but clients need an external URL.
   */
  private String authorizationServerUrl;

  /** Returns the authorization server URL for discovery, defaulting to issuer if not set. */
  public String getEffectiveAuthorizationServer() {
    if (authorizationServerUrl != null && !authorizationServerUrl.isBlank()) {
      return authorizationServerUrl;
    }
    return issuer;
  }

  /** Returns the effective JWKS URI, either the explicit one or derived from issuer. */
  public String getEffectiveJwksUri() {
    if (jwksUri != null && !jwksUri.isBlank()) {
      return jwksUri;
    }
    if (issuer == null) {
      return null;
    }
    var baseIssuer = issuer.endsWith("/") ? issuer : issuer + "/";
    return baseIssuer + ".well-known/jwks.json";
  }
}

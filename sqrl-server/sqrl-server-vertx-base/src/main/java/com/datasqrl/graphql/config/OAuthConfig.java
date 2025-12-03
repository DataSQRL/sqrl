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
import com.fasterxml.jackson.annotation.JsonSetter;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * OAuth 2.0 configuration combining Vert.x OAuth2Options with discovery metadata. This allows
 * reusing Vert.x's OAuth2Options for authentication while adding fields needed for RFC 9728
 * Protected Resource Metadata.
 */
@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OAuthConfig {

  /** Vert.x OAuth2Options for authentication provider creation. */
  private OAuth2Options oauth2Options;

  /**
   * External authorization server URL for discovery metadata. Use when the internal OAuth2Options
   * site URL is not reachable by external clients (e.g., Docker internal hostname vs localhost).
   */
  private String authorizationServerUrl;

  /** Scopes supported by this resource server. */
  private List<String> scopesSupported = List.of("mcp:tools", "mcp:resources");

  /** Resource identifier for this server (derived from request if not set). */
  private String resource;

  @JsonSetter("oauth2Options")
  public void setOauth2OptionsFromJson(Map<String, Object> options) {
    this.oauth2Options = options == null ? null : new OAuth2Options(new JsonObject(options));
  }

  /** Returns the site URL from OAuth2Options, or null if not configured. */
  public String getSite() {
    return oauth2Options != null ? oauth2Options.getSite() : null;
  }

  /** Returns the authorization server URL for discovery, defaulting to site if not set. */
  public String getEffectiveAuthorizationServer() {
    if (authorizationServerUrl != null && !authorizationServerUrl.isBlank()) {
      return authorizationServerUrl;
    }
    var site = getSite();
    if (site == null) {
      return null;
    }
    return site.endsWith("/") ? site : site + "/";
  }
}

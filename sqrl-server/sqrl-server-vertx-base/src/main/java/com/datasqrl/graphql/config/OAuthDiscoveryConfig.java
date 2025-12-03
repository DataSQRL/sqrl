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
 * Configuration for OAuth 2.0 Protected Resource Metadata (RFC 9728). These settings control what
 * is returned in the /.well-known/oauth-protected-resource endpoint.
 */
@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OAuthDiscoveryConfig {

  /**
   * External authorization server URL for discovery metadata. Use when the internal OAuth2Options
   * site URL is not reachable by external clients (e.g., Docker internal hostname vs localhost).
   */
  private String authorizationServerUrl;

  /** Scopes supported by this resource server. */
  private List<String> scopesSupported = List.of("mcp:tools", "mcp:resources");

  /** Resource identifier for this server (derived from request if not set). */
  private String resource;
}

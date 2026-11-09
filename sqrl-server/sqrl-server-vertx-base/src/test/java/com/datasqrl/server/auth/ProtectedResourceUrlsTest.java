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
package com.datasqrl.server.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.server.config.ProtectedResourceMetadata;
import io.vertx.core.http.HttpServerRequest;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProtectedResourceUrlsTest {

  private static final String MCP_ENDPOINT = "/v1/mcp";

  @Test
  void givenNoConfiguredResource_whenBuildingUrls_thenUsesRequestOrigin() {
    var metadata = metadata(null);
    var request = request("http", "localhost:8888", null);

    assertThat(ProtectedResourceUrls.resource(metadata, MCP_ENDPOINT, request))
        .isEqualTo("http://localhost:8888/v1/mcp");
    assertThat(ProtectedResourceUrls.metadataUrl(metadata, request))
        .isEqualTo("http://localhost:8888/.well-known/oauth-protected-resource");
  }

  @Test
  void givenNoConfiguredResourceAndForwardedProto_whenBuildingUrls_thenUsesForwardedScheme() {
    var metadata = metadata(" ");
    var request = request("http", "api.example.com", "https");

    assertThat(ProtectedResourceUrls.resource(metadata, MCP_ENDPOINT, request))
        .isEqualTo("https://api.example.com/v1/mcp");
    assertThat(ProtectedResourceUrls.metadataUrl(metadata, request))
        .isEqualTo("https://api.example.com/.well-known/oauth-protected-resource");
  }

  @Test
  void givenConfiguredResourceBehindHttpProxy_whenBuildingUrls_thenUsesResourceOrigin() {
    var metadata = metadata("https://api.example.com/v1/mcp");
    var request = request("http", "api.example.com", "http");

    assertThat(ProtectedResourceUrls.resource(metadata, MCP_ENDPOINT, request))
        .isEqualTo("https://api.example.com/v1/mcp");
    assertThat(ProtectedResourceUrls.metadataUrl(metadata, request))
        .isEqualTo("https://api.example.com/.well-known/oauth-protected-resource");
  }

  @Test
  void givenConfiguredResourceWithPort_whenBuildingMetadataUrl_thenKeepsPort() {
    var metadata = metadata("https://api.example.com:8443/v1/mcp");
    var request = request("http", "sqrl-server:8888", null);

    assertThat(ProtectedResourceUrls.metadataUrl(metadata, request))
        .isEqualTo("https://api.example.com:8443/.well-known/oauth-protected-resource");
  }

  @Test
  void givenConfiguredResourceWithoutScheme_whenBuildingMetadataUrl_thenUsesRequestOrigin() {
    var metadata = metadata("api.example.com/v1/mcp");
    var request = request("http", "localhost:8888", null);

    assertThat(ProtectedResourceUrls.metadataUrl(metadata, request))
        .isEqualTo("http://localhost:8888/.well-known/oauth-protected-resource");
  }

  static ProtectedResourceMetadata metadata(String resource) {
    return new ProtectedResourceMetadata(List.of("https://idp.example.com/"), resource, List.of());
  }

  static HttpServerRequest request(String scheme, String host, String forwardedProto) {
    var request = mock(HttpServerRequest.class);
    when(request.scheme()).thenReturn(scheme);
    when(request.getHeader("Host")).thenReturn(host);
    when(request.getHeader("X-Forwarded-Proto")).thenReturn(forwardedProto);
    return request;
  }
}

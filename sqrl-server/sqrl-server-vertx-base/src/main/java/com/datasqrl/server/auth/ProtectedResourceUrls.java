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

import com.datasqrl.server.config.ServerConfig;
import io.vertx.core.http.HttpServerRequest;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

/**
 * Builds the public URLs of the MCP endpoint as an OAuth 2.0 protected resource (RFC 9728). The
 * discovery metadata and the {@code WWW-Authenticate} header of 401 responses both use it, so they
 * always agree.
 */
@UtilityClass
class ProtectedResourceUrls {

  static final String METADATA_PATH = "/.well-known/oauth-protected-resource";

  /** Returns the configured resource, or the MCP endpoint URL derived from the request. */
  String resource(ServerConfig config, HttpServerRequest request) {
    var resource = config.getOauthConfig().getResource();
    if (StringUtils.isNotBlank(resource)) {
      return resource;
    }

    return requestOrigin(request) + config.getServletConfig().getMcpEndpoint("v1");
  }

  /**
   * Returns the protected resource metadata URL on the origin of the configured resource, or on the
   * origin of the request if no resource is configured.
   */
  String metadataUrl(ServerConfig config, HttpServerRequest request) {
    return configuredResourceOrigin(config).orElseGet(() -> requestOrigin(request)) + METADATA_PATH;
  }

  private Optional<String> configuredResourceOrigin(ServerConfig config) {
    var resource = config.getOauthConfig().getResource();
    if (StringUtils.isBlank(resource)) {
      return Optional.empty();
    }

    try {
      var uri = new URI(resource);
      if (uri.getScheme() == null || uri.getHost() == null) {
        return Optional.empty();
      }
      var port = uri.getPort() == -1 ? "" : ":" + uri.getPort();
      return Optional.of(uri.getScheme() + "://" + uri.getHost() + port);
    } catch (URISyntaxException e) {
      return Optional.empty();
    }
  }

  private String requestOrigin(HttpServerRequest request) {
    var forwardedProto = request.getHeader("X-Forwarded-Proto");
    var scheme = forwardedProto != null ? forwardedProto : request.scheme();

    return scheme + "://" + request.getHeader("Host");
  }
}

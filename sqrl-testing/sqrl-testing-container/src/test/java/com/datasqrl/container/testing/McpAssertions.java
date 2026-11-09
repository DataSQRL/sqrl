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
package com.datasqrl.container.testing;

import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.transport.McpHttpClientTransportAuthorizationException;
import java.util.Objects;
import java.util.stream.Stream;

/** Assertions on failures of the MCP client in container tests. */
final class McpAssertions {

  private McpAssertions() {}

  /** Asserts that the MCP client failed with an HTTP authorization error of the given status. */
  static void assertMcpAuthorizationStatus(Throwable ex, int statusCode) {
    var authException =
        Stream.iterate(ex, Objects::nonNull, Throwable::getCause)
            .filter(McpHttpClientTransportAuthorizationException.class::isInstance)
            .map(McpHttpClientTransportAuthorizationException.class::cast)
            .findFirst();
    assertThat(authException)
        .as("MCP authorization error in the cause chain of %s", ex)
        .isPresent();
    assertThat(authException.get().getResponseInfo().statusCode()).isEqualTo(statusCode);
  }
}

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
package com.datasqrl.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.Test;

class ForwardedClientAddressLogFormatterTest {

  @Test
  void givenForwardedClientAddress_whenFormatting_thenUsesVertxDefaultLogLayout() {
    var context = mock(RoutingContext.class);
    var request = mock(HttpServerRequest.class);
    var response = mock(HttpServerResponse.class);
    when(context.request()).thenReturn(request);
    when(request.remoteAddress()).thenReturn(SocketAddress.inetSocketAddress(8080, "10.11.5.76"));
    when(request.getHeader("X-Forwarded-For")).thenReturn("1.2.3.4");
    when(request.getHeader("Referer")).thenReturn("https://example.com");
    when(request.getHeader("User-Agent")).thenReturn("test-client");
    when(request.method()).thenReturn(HttpMethod.POST);
    when(request.uri()).thenReturn("/graphql");
    when(request.version()).thenReturn(HttpVersion.HTTP_1_1);
    when(request.response()).thenReturn(response);
    when(response.getStatusCode()).thenReturn(200);
    when(response.bytesWritten()).thenReturn(42L);

    var logLine =
        new ForwardedClientAddressLogFormatter(new ClientAddressResolver(true)).format(context, 0);

    assertThat(logLine)
        .matches(
            "1\\.2\\.3\\.4 - - \\[.+] \\\"POST /graphql HTTP/1\\.1\\\" 200 42 \\\"https://example\\.com\\\" \\\"test-client\\\"");
  }
}

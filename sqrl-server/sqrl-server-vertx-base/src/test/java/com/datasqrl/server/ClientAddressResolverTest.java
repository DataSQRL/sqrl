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

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import org.junit.jupiter.api.Test;

class ClientAddressResolverTest {

  @Test
  void givenForwardedAddressDisabled_whenResolvingAddress_thenReturnsPeerAddress() {
    var request = requestFrom("203.0.113.10", "198.51.100.20");

    var address = new ClientAddressResolver(false).resolve(request);

    assertThat(address).isEqualTo("203.0.113.10");
  }

  @Test
  void givenForwardedAddressEnabled_whenResolvingAddress_thenReturnsOriginalClient() {
    var request = requestFrom("10.11.5.76", "198.51.100.20, 10.11.5.76");

    var address = new ClientAddressResolver(true).resolve(request);

    assertThat(address).isEqualTo("198.51.100.20");
  }

  @Test
  void givenInvalidForwardedAddress_whenResolvingAddress_thenReturnsPeerAddress() {
    var request = requestFrom("10.11.5.76", "not-an-address");

    var address = new ClientAddressResolver(true).resolve(request);

    assertThat(address).isEqualTo("10.11.5.76");
  }

  @Test
  void givenShorthandNumericForwardedAddress_whenResolvingAddress_thenReturnsPeerAddress() {
    var request = requestFrom("10.11.5.76", "123");

    var address = new ClientAddressResolver(true).resolve(request);

    assertThat(address).isEqualTo("10.11.5.76");
  }

  private HttpServerRequest requestFrom(String peerAddress, String forwardedFor) {
    var request = mock(HttpServerRequest.class);
    when(request.remoteAddress()).thenReturn(SocketAddress.inetSocketAddress(8080, peerAddress));
    when(request.getHeader("X-Forwarded-For")).thenReturn(forwardedFor);
    return request;
  }
}

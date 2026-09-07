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

import com.google.common.net.InetAddresses;
import io.vertx.core.http.HttpServerRequest;
import java.net.InetAddress;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/** Resolves the client address from the configured request source. */
public class ClientAddressResolver {

  private final boolean logForwardedClientAddress;

  public ClientAddressResolver(boolean logForwardedClientAddress) {
    this.logForwardedClientAddress = logForwardedClientAddress;
  }

  public String resolve(HttpServerRequest request) {
    var remoteAddress = request.remoteAddress();
    if (remoteAddress == null) {
      return "unknown";
    }

    var peerAddress = remoteAddress.host();
    if (!logForwardedClientAddress) {
      return peerAddress;
    }

    return firstForwardedAddress(request.getHeader("X-Forwarded-For")).orElse(peerAddress);
  }

  private Optional<String> firstForwardedAddress(String forwardedFor) {
    if (StringUtils.isBlank(forwardedFor)) {
      return Optional.empty();
    }

    var firstAddress = forwardedFor.split(",", 2)[0].trim();

    return parseAddress(firstAddress).map(ignored -> firstAddress);
  }

  private static Optional<InetAddress> parseAddress(String value) {
    if (!InetAddresses.isInetAddress(value)) {
      return Optional.empty();
    }

    return Optional.of(InetAddresses.forString(value));
  }
}

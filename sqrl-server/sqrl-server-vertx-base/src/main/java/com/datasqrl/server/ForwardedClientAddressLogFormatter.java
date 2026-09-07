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

import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.LoggerFormatter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import lombok.RequiredArgsConstructor;

/** Formats Vert.x's default access log layout with the resolved client address. */
@RequiredArgsConstructor
public class ForwardedClientAddressLogFormatter implements LoggerFormatter {

  private final ClientAddressResolver clientAddressResolver;

  @Override
  public String format(RoutingContext context, long duration) {
    var request = context.request();
    var response = request.response();
    var referrer = request.getHeader("Referrer");
    if (referrer == null) {
      referrer = request.getHeader("Referer");
    }
    var userAgent = request.getHeader("User-Agent");

    return String.format(
        "%s - - [%s] \"%s %s %s\" %d %d \"%s\" \"%s\"",
        clientAddressResolver.resolve(request),
        DateTimeFormatter.RFC_1123_DATE_TIME.format(
            Instant.ofEpochMilli(System.currentTimeMillis() - duration).atZone(ZoneOffset.UTC)),
        request.method(),
        request.uri(),
        formatHttpVersion(request.version()),
        response.getStatusCode(),
        response.bytesWritten(),
        referrer == null ? "-" : referrer,
        userAgent == null ? "-" : userAgent);
  }

  private String formatHttpVersion(HttpVersion version) {
    return switch (version) {
      case HTTP_1_0 -> "HTTP/1.0";
      case HTTP_1_1 -> "HTTP/1.1";
      case HTTP_2 -> "HTTP/2.0";
      default -> "-";
    };
  }
}

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
package com.datasqrl.graphql.auth;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
public class JwtFailureHandler implements Handler<RoutingContext> {

  @Override
  public void handle(RoutingContext ctx) {
    var throwable = ctx.failure();
    var statusCode = ctx.statusCode();
    var errorMsg = "JWT auth failed";
    var errorCause =
        throwable == null ? "Unknown error" : ExceptionUtils.getRootCauseMessage(throwable);

    log.warn(
        "JWT authentication failed for request {} {}: {} (status: {})",
        ctx.request().method(),
        ctx.request().path(),
        errorCause,
        statusCode);

    if (throwable != null) {
      log.debug("JWT authentication failure details", throwable);
    }

    var response =
        ctx.response()
            .setStatusCode(statusCode != -1 ? statusCode : 401)
            .putHeader("Content-Type", "application/json");

    var json = new JsonObject().put("error", errorMsg).put("cause", errorCause);
    response.end(json.encode());
  }
}

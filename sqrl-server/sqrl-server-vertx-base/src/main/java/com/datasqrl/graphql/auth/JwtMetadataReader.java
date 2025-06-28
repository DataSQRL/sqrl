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

import com.datasqrl.graphql.server.MetadataReader;
import com.google.common.base.Preconditions;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.ext.auth.impl.jose.JWT;
import io.vertx.ext.web.RoutingContext;

public class JwtMetadataReader implements MetadataReader {

  @Override
  public Object read(DataFetchingEnvironment env, String name, boolean isRequired) {
    RoutingContext rc = env.getGraphQlContext().get(RoutingContext.class);

    var headers = rc.request().headers();
    var auth = headers.get("Authorization");

    if (auth == null || !auth.startsWith("Bearer ")) {
      throw new IllegalStateException("Missing bearer token");
    }

    // no need to validate, if we reached this, Authorization already validated the tokens
    var claims = JWT.parse(auth.substring("Bearer ".length()));
    var payload = claims.getJsonObject("payload");

    if (isRequired && !payload.containsKey(name)) {
      throw new IllegalArgumentException(
          "Claim '%s' is not present on payload. Claims available: %s"
              .formatted(name, claims.fieldNames()));
    }

    var value = payload.getValue(name);

    if (isRequired) {
      Preconditions.checkNotNull(value, "Claim '%s' must not be null", name);
    }

    return value;
  }
}

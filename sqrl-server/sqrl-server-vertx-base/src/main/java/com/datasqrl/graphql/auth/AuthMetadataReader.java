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

import static com.google.common.base.Preconditions.checkNotNull;

import com.datasqrl.graphql.server.MetadataReader;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuthMetadataReader implements MetadataReader {

  @Override
  public Object read(DataFetchingEnvironment env, String name, boolean isRequired) {
    RoutingContext rc = env.getGraphQlContext().get(RoutingContext.class);
    var principal = rc.user();

    if (principal == null) {
      throw new IllegalStateException("Not authenticated");
    }

    if (isRequired && !principal.containsKey(name)) {
      log.warn(
          "Attribute {} is not present on authorization, attributes: {}",
          name,
          principal.attributes());
      throw new IllegalArgumentException(
          "Attribute '%s' is not present on authorization".formatted(name));
    }

    var value = principal.get(name);

    if (isRequired) {
      checkNotNull(value, "Claim '%s' must not be null", name);
    }

    return value;
  }
}

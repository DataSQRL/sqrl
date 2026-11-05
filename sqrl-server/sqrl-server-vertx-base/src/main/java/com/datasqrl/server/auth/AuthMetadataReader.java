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

import static com.google.common.base.Preconditions.checkNotNull;

import com.datasqrl.server.MetadataReader;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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

    var resolvedClaimValue = findClaim(principal, name);

    if (isRequired && resolvedClaimValue.isEmpty()) {
      log.warn(
          "Required claim '{}' is not present on authorization, attributes: {}",
          name,
          principal.attributes());

      // Set the HTTP status to 403 directly
      rc.response().setStatusCode(403);
      rc.response()
          .putHeader(
              "WWW-Authenticate",
              "Bearer error=\"insufficient_scope\", error_description=\"Required claim missing\"");

      var locations =
          env.getField().getSourceLocation() == null
              ? null
              : List.of(env.getField().getSourceLocation());

      throw new MissingRequiredClaimException(
          name, locations, env.getExecutionStepInfo().getPath());
    }

    var value = resolvedClaimValue.map(ResolvedClaimValue::value).orElse(null);

    if (isRequired) {
      checkNotNull(value, "Claim '%s' must not be null", name);
    }

    return value;
  }

  /**
   * Finds the claim with the given name. A dot in the name steps into a nested object claim, so
   * 'org.id' reads the field 'id' of the claim 'org'. A claim name can also contain dots, for
   * example 'https://example.com/org'. So the longest prefix of the name that is a top-level claim
   * is used, and the rest of the name is the path into that claim.
   *
   * @return the claim value, or empty when the token has no claim with this name
   */
  private static Optional<ResolvedClaimValue> findClaim(User principal, String claimName) {
    var segments = claimName.split("\\.");
    for (var prefixLength = segments.length; prefixLength >= 1; prefixLength--) {
      var topLevelClaimName = String.join(".", Arrays.copyOfRange(segments, 0, prefixLength));
      if (principal.containsKey(topLevelClaimName)) {
        var pathSegments = Arrays.copyOfRange(segments, prefixLength, segments.length);
        return walkPath(principal.get(topLevelClaimName), pathSegments);
      }
    }
    return Optional.empty();
  }

  /**
   * Steps from the top-level claim value into the nested object field of each path segment, in
   * order. A step into a value that is not an object, such as an array, finds no claim.
   */
  private static Optional<ResolvedClaimValue> walkPath(
      Object topLevelClaimValue, String[] pathSegments) {
    var currentValue = topLevelClaimValue;
    for (var pathSegment : pathSegments) {
      if (!(currentValue instanceof JsonObject currentObject)
          || !currentObject.containsKey(pathSegment)) {
        return Optional.empty();
      }
      currentValue = currentObject.getValue(pathSegment);
    }
    return Optional.of(new ResolvedClaimValue(currentValue));
  }

  /**
   * The value of a claim that the token contains. The value can be null, when the token sets the
   * claim to null.
   */
  private record ResolvedClaimValue(Object value) {}
}

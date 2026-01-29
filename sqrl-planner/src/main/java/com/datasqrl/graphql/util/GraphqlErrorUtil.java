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
package com.datasqrl.graphql.util;

import graphql.language.SourceLocation;
import graphql.validation.ValidationError;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class GraphqlErrorUtil {

  public static String formatValidationError(ValidationError error) {
    var type = String.valueOf(error.getValidationErrorType());
    var message = error.getMessage();
    var locations =
        error.getLocations() == null || error.getLocations().isEmpty()
            ? ""
            : " (at "
                + error.getLocations().stream()
                    .map(SourceLocation::toString)
                    .collect(Collectors.joining(", "))
                + ")";

    return type + ": " + message + locations;
  }
}

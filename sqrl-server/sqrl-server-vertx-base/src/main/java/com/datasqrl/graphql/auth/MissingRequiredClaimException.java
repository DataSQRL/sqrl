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

import graphql.GraphQLError;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;
import java.util.List;
import lombok.Getter;

/**
 * Exception thrown when a required JWT claim is missing or not present in the authorization token.
 * This exception should result in an HTTP 403 Forbidden response.
 */
@Getter
public class MissingRequiredClaimException extends RuntimeException implements GraphQLError {

  private final String claimName;
  private final List<SourceLocation> locations;
  private final ResultPath path;

  public MissingRequiredClaimException(String claimName) {
    super("Required claim missing");
    this.claimName = claimName;
    this.locations = null;
    this.path = null;
  }

  public MissingRequiredClaimException(
      String claimName, List<SourceLocation> locations, ResultPath path) {
    super("Required claim missing");
    this.claimName = claimName;
    this.locations = locations;
    this.path = path;
  }

  @Override
  public String getMessage() {
    return "Forbidden";
  }

  @Override
  public List<SourceLocation> getLocations() {
    return locations;
  }

  @Override
  public graphql.ErrorClassification getErrorType() {
    return graphql.ErrorType.ValidationError;
  }

  @Override
  public List<Object> getPath() {
    return path != null ? path.toList() : null;
  }
}

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
package com.datasqrl.graphql.server.operation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import graphql.language.OperationDefinition.Operation;
import java.util.Objects;
import lombok.NonNull;
import lombok.Value;

/**
 * Represents an API function or tool that. It contains the {@link FunctionDefinition} that is
 * passed to the LLM as a tool and the {@link GraphQLQuery} that is executed.
 */
@Value
public class ApiOperation {

  @NonNull FunctionDefinition function;
  @NonNull GraphQLQuery apiQuery;
  boolean isTool; // MCP Tool
  boolean isResource; // MCP Resource
  boolean isRestEndpoint; // REST Resource

  /** RFC 6570 URI template for Resource endpoint (both MCP and REST) */
  String uriTemplate;

  HttpMethod httpMethod; // REST Method

  public ApiOperation(FunctionDefinition function, GraphQLQuery apiQuery) {
    this(
        function,
        apiQuery,
        true,
        false,
        apiQuery.operationType() != Operation.SUBSCRIPTION,
        getDefaultUriTemplate(function, apiQuery),
        getDefaultHttpMethod(apiQuery));
  }

  @JsonCreator
  public ApiOperation(
      @JsonProperty("function") FunctionDefinition function,
      @JsonProperty("apiQuery") GraphQLQuery apiQuery,
      @JsonProperty("tool") boolean isTool,
      @JsonProperty("resource") boolean isResource,
      @JsonProperty("restEndpoint") boolean isRestEndpoint,
      @JsonProperty("uriTemplate") String uriTemplate,
      @JsonProperty("httpMethod") HttpMethod httpMethod) {
    this.function = function;
    this.apiQuery = apiQuery;
    this.isTool = isTool;
    this.isResource = isResource;
    this.isRestEndpoint = isRestEndpoint;
    this.uriTemplate = uriTemplate;
    this.httpMethod = httpMethod;
  }

  /**
   * Uniquely identifies an operation
   *
   * @return
   */
  @JsonIgnore
  public String getId() {
    return function.getName();
  }

  @JsonIgnore
  public String getName() {
    return function.getName();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ApiOperation that = (ApiOperation) o;
    return Objects.equals(getId(), that.getId());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getId());
  }

  public static HttpMethod getDefaultHttpMethod(GraphQLQuery apiQuery) {
    return switch (apiQuery.operationType()) {
      case QUERY -> HttpMethod.GET;
      case MUTATION -> HttpMethod.POST;
      case SUBSCRIPTION -> null;
    };
  }

  public static String getDefaultUriTemplate(FunctionDefinition function, GraphQLQuery apiQuery) {
    return switch (apiQuery.operationType()) {
      case QUERY -> UriTemplateHelper.getQueryUriTemplate(function, apiQuery.queryName());
      case MUTATION -> UriTemplateHelper.getMutationUriTemplate(apiQuery.queryName());
      case SUBSCRIPTION -> null;
    };
  }
}

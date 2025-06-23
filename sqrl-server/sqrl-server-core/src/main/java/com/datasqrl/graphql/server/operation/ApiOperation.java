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
import java.util.Objects;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Represents an API function or tool that. It contains the {@link FunctionDefinition} that is
 * passed to the LLM as a tool and the {@link GraphQLQuery} that is executed.
 */
@Value
@Builder
public class ApiOperation {

  @NonNull FunctionDefinition function;
  @NonNull GraphQLQuery apiQuery;
  McpMethodType mcpMethod; // The MCP endpoint type or NONE if no MCP
  RestMethodType restMethod; // The REST Method for the operation or NONE if no REST

  /** RFC 6570 URI template for Resource endpoint (both MCP and REST) */
  String uriTemplate;

  ResultFormat format;

  public static ApiOperationBuilder getBuilder(FunctionDefinition function, GraphQLQuery apiQuery) {
    return builder()
        .function(function)
        .apiQuery(apiQuery)
        .mcpMethod(getDefaultMcpMethod(function))
        .restMethod(getDefaultRestMethod(apiQuery))
        .uriTemplate(getDefaultUriTemplate(function, apiQuery))
        .format(ResultFormat.JSON);
  }

  @JsonCreator
  public ApiOperation(
      @JsonProperty("function") FunctionDefinition function,
      @JsonProperty("query") GraphQLQuery apiQuery,
      @JsonProperty("mcp") McpMethodType mcpMethod,
      @JsonProperty("rest") RestMethodType restMethod,
      @JsonProperty("uri") String uriTemplate,
      @JsonProperty("format") ResultFormat format) {
    this.function = function;
    this.apiQuery = apiQuery;
    this.mcpMethod = mcpMethod;
    this.restMethod = restMethod;
    this.uriTemplate = uriTemplate;
    this.format = format;
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

  @JsonIgnore
  public boolean isRestEndpoint() {
    return restMethod != RestMethodType.NONE;
  }

  @JsonIgnore
  public boolean isMcpEndpoint() {
    return mcpMethod != McpMethodType.NONE;
  }

  /**
   * Whether to remove the operation-level nesting of result data for GraphQL queries for this
   * operation when returned through the bridge TODO: make configurable
   *
   * @return
   */
  @JsonIgnore
  public boolean removeNesting() {
    return true;
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

  public static McpMethodType getDefaultMcpMethod(FunctionDefinition function) {
    if (function.getParameters() == null || !function.getParameters().isNested()) {
      return McpMethodType.TOOL;
    } else {
      return McpMethodType.NONE;
    }
  }

  public static RestMethodType getDefaultRestMethod(GraphQLQuery apiQuery) {
    return switch (apiQuery.operationType()) {
      case QUERY -> RestMethodType.GET;
      case MUTATION -> RestMethodType.POST;
      case SUBSCRIPTION -> RestMethodType.NONE;
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

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
package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.ComputedParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.MetadataParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.ParameterHandlerVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.ParentParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryParameterHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public abstract class AbstractQueryExecutionContext
    implements QueryExecutionContext, ParameterHandlerVisitor<Object, QueryExecutionContext> {

  public static Object unboxList(List o, boolean isList) {
    return isList ? o : (o.size() > 0 ? o.get(0) : null);
  }

  @SneakyThrows
  @Override
  public Object visitParentParameter(
      ParentParameter parentParameter, QueryExecutionContext context) {
    return context
        .getContext()
        .createPropertyFetcher(parentParameter.getKey())
        .get(context.getEnvironment());
  }

  @Override
  public Object visitArgumentParameter(
      ArgumentParameter argumentParameter, QueryExecutionContext context) {
    return context.getArguments().stream()
        .filter(arg -> arg.getPath().equalsIgnoreCase(argumentParameter.getPath()))
        .findFirst()
        .map(Argument::getValue)
        .orElse(null);
  }

  @Override
  public Object visitComputedParameter(
      ComputedParameter computedParameter, QueryExecutionContext context) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Object visitMetadataParameter(
      MetadataParameter metadataParameter, QueryExecutionContext context) {

    var md = metadataParameter.getMetadata();
    return context
        .getContext()
        .getMetadataReader(md.metadataType())
        .read(context.getEnvironment(), md.name(), md.isRequired());
  }

  public List getParamArguments(List<QueryParameterHandler> parameters) {
    return parameters.stream()
        .map(param -> param.accept(this, this))
        .collect(Collectors.toCollection(() -> new ArrayList<>(parameters.size() + 2)));
  }

  public static String addLimitOffsetToQuery(String sqlQuery, String limit, String offset) {
    return "SELECT * FROM (%s) x LIMIT %s OFFSET %s".formatted(sqlQuery, limit, offset);
  }
}

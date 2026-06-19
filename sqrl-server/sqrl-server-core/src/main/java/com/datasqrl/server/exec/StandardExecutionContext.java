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
package com.datasqrl.server.exec;

import com.datasqrl.server.ServerContext;
import com.datasqrl.server.graphql.RootGraphQLModel.Argument;
import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentParameter;
import com.datasqrl.server.graphql.RootGraphQLModel.ComputedParameter;
import com.datasqrl.server.graphql.RootGraphQLModel.MetadataParameter;
import com.datasqrl.server.graphql.RootGraphQLModel.ParameterHandlerVisitor;
import com.datasqrl.server.graphql.RootGraphQLModel.ParentParameter;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

@AllArgsConstructor
@Getter
public class StandardExecutionContext<C extends ServerContext>
    implements ExecutionContext, ParameterHandlerVisitor<Object, ExecutionContext> {

  protected final C serverContext;
  protected final DataFetchingEnvironment environment;
  protected final Set<Argument> arguments;

  public static Object unboxList(List o, boolean isList) {
    return isList ? o : (o.size() > 0 ? o.get(0) : null);
  }

  @SneakyThrows
  @Override
  public Object visitParentParameter(ParentParameter parentParameter, ExecutionContext execCtx) {
    return execCtx
        .getServerContext()
        .createPropertyFetcher(parentParameter.getKey())
        .get(execCtx.getEnvironment());
  }

  @Override
  public Object visitArgumentParameter(
      ArgumentParameter argumentParameter, ExecutionContext execCtx) {
    return execCtx.getArguments().stream()
        .filter(arg -> arg.getPath().equalsIgnoreCase(argumentParameter.getPath()))
        .findFirst()
        .map(Argument::getValue)
        .orElse(null);
  }

  @Override
  public Object visitMetadataParameter(
      MetadataParameter metadataParameter, ExecutionContext execCtx) {
    var md = metadataParameter.getMetadata();
    return execCtx
        .getServerContext()
        .getMetadataReader(md.metadataType())
        .read(execCtx.getEnvironment(), md.name(), md.required());
  }

  @Override
  public CompletableFuture<Object> visitComputedParameter(
      ComputedParameter computedParameter, ExecutionContext execCtx) {
    var fnId = computedParameter.getFunctionId();

    return execCtx.getServerContext().getFunctionExecutor().execute(execCtx.getEnvironment(), fnId);
  }
}

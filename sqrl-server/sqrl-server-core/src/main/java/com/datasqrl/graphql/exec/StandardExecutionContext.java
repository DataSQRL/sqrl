package com.datasqrl.graphql.exec;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.MetadataParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.ParameterHandlerVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.ParentParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryParameterHandler;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

@AllArgsConstructor
@Getter
public class StandardExecutionContext<C extends Context>
    implements ExecutionContext, ParameterHandlerVisitor<Object, ExecutionContext> {

  final C context;
  final DataFetchingEnvironment environment;
  final Set<Argument> arguments;

  public static Object unboxList(List o, boolean isList) {
    return isList ? o : (o.size() > 0 ? o.get(0) : null);
  }

  @SneakyThrows
  @Override
  public Object visitParentParameter(ParentParameter parentParameter, ExecutionContext context) {
    return context
        .getContext()
        .createPropertyFetcher(parentParameter.getKey())
        .get(context.getEnvironment());
  }

  @Override
  public Object visitArgumentParameter(
      ArgumentParameter argumentParameter, ExecutionContext context) {
    return context.getArguments().stream()
        .filter(arg -> arg.getPath().equalsIgnoreCase(argumentParameter.getPath()))
        .findFirst()
        .map(Argument::getValue)
        .orElse(null);
  }

  @Override
  public Object visitMetadataParameter(
      MetadataParameter metadataParameter, ExecutionContext context) {
    var md = metadataParameter.getMetadata();
    return context
        .getContext()
        .getMetadataReader(md.metadataType())
        .read(context.getEnvironment(), md.name(), md.isRequired());
  }

  public List<Object> getParamArguments(List<QueryParameterHandler> parameters) {
    return parameters.stream()
        .map(param -> param.accept(this, this))
        .collect(Collectors.toCollection(() -> new ArrayList<>(parameters.size() + 2)));
  }
}

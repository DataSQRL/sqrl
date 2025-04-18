package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryParameterHandler;
import com.datasqrl.graphql.server.RootGraphqlModel.ParameterHandlerVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.SourceParameter;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;

public abstract class AbstractQueryExecutionContext implements QueryExecutionContext,
    ParameterHandlerVisitor<Object, QueryExecutionContext> {

  public static Object unboxList(List o, boolean isList) {
    return isList
        ? o
        : (o.size() > 0 ? o.get(0) : null);
  }

  @SneakyThrows
  @Override
  public Object visitSourceParameter(SourceParameter sourceParameter,
      QueryExecutionContext context) {
    return context.getContext().createPropertyFetcher(sourceParameter.getKey())
        .get(context.getEnvironment());
  }

  @Override
  public Object visitArgumentParameter(ArgumentParameter argumentParameter,
      QueryExecutionContext context) {
    return context.getArguments().stream()
        .filter(arg -> arg.getPath().equalsIgnoreCase(argumentParameter.getPath()))
        .findFirst()
        .map(Argument::getValue)
        .orElse(null);
  }

  public List getParamArguments(List<QueryParameterHandler> parameters) {
    List paramObj = new ArrayList(parameters.size()+2);
    for (int i = 0; i < parameters.size(); i++) {
      QueryParameterHandler param = parameters.get(i);
      Object o = param.accept(this, this);
      paramObj.add(o);
    }
    assert paramObj.size() == parameters.size();
    return paramObj;
  }

  public static String addLimitOffsetToQuery(String sqlQuery, String limit, String offset) {
    return String.format("SELECT * FROM (%s) x LIMIT %s OFFSET %s", sqlQuery, limit, offset);
  }

}

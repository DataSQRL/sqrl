package com.datasqrl.graphql.server.query;

import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Getter
@NoArgsConstructor
public class ResolvedSqlQuery implements ResolvedQuery {

  SqlQuery query;
  PreparedQueryContainer preparedQueryContainer;
  Optional<Preprocessor> preprocessor;

  @Override
  public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context) {
    return visitor.visitResolvedSqlQuery(this, context);
  }

  @Override
  public ResolvedSqlQuery preprocess(QueryExecutionContext context) {
    return preprocessor.map(pre -> pre.preprocess(this, context)).orElse(this);
  }
}

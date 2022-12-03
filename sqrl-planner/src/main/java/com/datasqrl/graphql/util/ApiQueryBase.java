package com.datasqrl.graphql.util;

import com.datasqrl.graphql.inference.ArgumentSet;
import com.datasqrl.graphql.server.Model.PgParameterHandler;
import com.datasqrl.graphql.server.Model.QueryBase;
import com.datasqrl.graphql.server.Model.QueryBaseVisitor;
import com.datasqrl.plan.queries.APIQuery;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Singular;
import org.apache.calcite.rel.RelNode;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ApiQueryBase implements QueryBase {

  final String type = "pgQuery";
  APIQuery query;
  RelNode relNode;
  ArgumentSet relAndArg;
  @Singular
  List<PgParameterHandler> parameters;

  @Override
  public <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context) {
    ApiQueryVisitor<R, C> visitor1 = (ApiQueryVisitor<R, C>) visitor;
    return visitor1.visitApiQuery(this, context);
  }
}
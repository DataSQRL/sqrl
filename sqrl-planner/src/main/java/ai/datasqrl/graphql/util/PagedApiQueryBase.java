package ai.datasqrl.graphql.util;

import ai.datasqrl.graphql.inference.ArgumentSet;
import ai.datasqrl.graphql.server.Model.*;
import ai.datasqrl.plan.queries.APIQuery;
import lombok.*;
import org.apache.calcite.rel.RelNode;

import java.util.*;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class PagedApiQueryBase implements QueryBase {

  final String type = "pagedPgQuery";
  APIQuery query;
  RelNode relNode;
  ArgumentSet relAndArg;
  @Singular
  List<PgParameterHandler> parameters;

  @Override
  public <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context) {
    ApiQueryVisitor<R, C> visitor1 = (ApiQueryVisitor<R, C>) visitor;
    return visitor1.visitPagedApiQuery(this, context);
  }
}
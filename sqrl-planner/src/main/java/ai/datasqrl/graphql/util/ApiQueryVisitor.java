package ai.datasqrl.graphql.util;

import ai.datasqrl.graphql.server.Model.QueryBaseVisitor;

public interface ApiQueryVisitor<R, C> extends QueryBaseVisitor<R, C> {

  R visitApiQuery(ApiQueryBase apiQueryBase, C context);

  R visitPagedApiQuery(PagedApiQueryBase apiQueryBase, C context);
}

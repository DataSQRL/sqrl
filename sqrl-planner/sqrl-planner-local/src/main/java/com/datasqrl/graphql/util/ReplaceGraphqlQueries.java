/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */

package com.datasqrl.graphql.util;

import com.datasqrl.SqrlRelToSql;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.DynamicParamSqlPrettyWriter;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.graphql.server.Model.*;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

public class ReplaceGraphqlQueries implements
    RootVisitor<Object, Object>,
    CoordVisitor<Object, Object>,
    SchemaVisitor<Object, Object>,
    GraphQLArgumentWrapperVisitor<Object, Object>,
    QueryBaseVisitor<JdbcQuery, Object>,
    ApiQueryVisitor<JdbcQuery, Object>,
    ResolvedQueryVisitor<Object, Object>,
    ParameterHandlerVisitor<Object, Object> {

  private final Map<IdentifiedQuery, QueryTemplate> queries;
  private final QueryPlanner planner;

  public ReplaceGraphqlQueries(Map<IdentifiedQuery, QueryTemplate> queries, QueryPlanner planner) {

    this.queries = queries;
    this.planner = planner;
  }

  @Override
  public JdbcQuery visitApiQuery(ApiQueryBase apiQueryBase, Object context) {
    QueryTemplate template = queries.get(apiQueryBase.getQuery());

    String query = planner.relToString(Dialect.POSTGRES,
        planner.convertRelToDialect(Dialect.POSTGRES, template.getRelNode()));

    return JdbcQuery.builder()
        .parameters(apiQueryBase.getParameters())
        .sql(query)
        .build();
  }

  @Override
  public JdbcQuery visitPagedApiQuery(PagedApiQueryBase apiQueryBase, Object context) {
    QueryTemplate template = queries.get(apiQueryBase.getQuery());

    //todo builder
    SqlWriterConfig config = SqrlRelToSql.transform.apply(SqlPrettyWriter.config());
    DynamicParamSqlPrettyWriter writer = new DynamicParamSqlPrettyWriter(config);
    String query = convertDynamicParamsWithWriter(writer, template.getRelNode());
    if (writer.getDynamicParameters().size() > 0) {
      Preconditions.checkState(
          Collections.max(writer.getDynamicParameters()) < apiQueryBase.getParameters().size());
    } else {
      Preconditions.checkState(apiQueryBase.getParameters().size() == 0);
    }
    return new PagedJdbcQuery(
        query,
        apiQueryBase.getParameters());
  }

  private String convertDynamicParamsWithWriter(DynamicParamSqlPrettyWriter writer, RelNode relNode) {
    SqlNode node = SqrlRelToSql.convertToSqlNode(relNode);
    node.unparse(writer, 0, 0);
    return writer.toSqlString().getSql();
  }

  @Override
  public Object visitRoot(RootGraphqlModel root, Object context) {
    root.getCoords().forEach(c -> c.accept(this, context));
    return null;
  }

  @Override
  public Object visitStringDefinition(StringSchema stringSchema, Object context) {
    return null;
  }

  @Override
  public Object visitArgumentLookup(ArgumentLookupCoords coords, Object context) {
    coords.getMatchs().forEach(c -> {
      JdbcQuery query = c.getQuery().accept(this, context);
      c.setQuery(query);
    });
    return null;
  }

  @Override
  public Object visitFieldLookup(FieldLookupCoords coords, Object context) {
    return null;
  }

  @Override
  public JdbcQuery visitJdbcQuery(JdbcQuery jdbcQuery, Object context) {
    return null;
  }

  @Override
  public JdbcQuery visitPagedJdbcQuery(PagedJdbcQuery jdbcQuery, Object context) {
    return null;
  }

  @Override
  public Object visitSourceParameter(SourceParameter sourceParameter, Object context) {
    return null;
  }

  @Override
  public Object visitArgumentParameter(ArgumentParameter argumentParameter, Object context) {
    return null;
  }

  @Override
  public Object visitResolvedJdbcQuery(ResolvedJdbcQuery query, Object context) {
    return null;
  }

  @Override
  public Object visitResolvedPagedJdbcQuery(ResolvedPagedJdbcQuery query, Object context) {
    return null;
  }

  @Override
  public Object visitArgumentWrapper(GraphQLArgumentWrapper graphQLArgumentWrapper,
      Object context) {
    return null;
  }
}
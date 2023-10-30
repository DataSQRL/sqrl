/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */

package com.datasqrl.graphql.util;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.ArgumentParameter;
import com.datasqrl.graphql.server.Model.CoordVisitor;
import com.datasqrl.graphql.server.Model.FieldLookupCoords;
import com.datasqrl.graphql.server.Model.JdbcQuery;
import com.datasqrl.graphql.server.Model.PagedJdbcQuery;
import com.datasqrl.graphql.server.Model.ParameterHandlerVisitor;
import com.datasqrl.graphql.server.Model.QueryBaseVisitor;
import com.datasqrl.graphql.server.Model.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.Model.ResolvedPagedJdbcQuery;
import com.datasqrl.graphql.server.Model.ResolvedQueryVisitor;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.RootVisitor;
import com.datasqrl.graphql.server.Model.SchemaVisitor;
import com.datasqrl.graphql.server.Model.SourceParameter;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class ReplaceGraphqlQueries implements
    RootVisitor<Object, Object>,
    CoordVisitor<Object, Object>,
    SchemaVisitor<Object, Object>,
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

    String query = planner.relToString(Dialect.POSTGRES,
        planner.convertRelToDialect(Dialect.POSTGRES, template.getRelNode()));

    int numDynamParams = countParameters(template.getRelNode());
    //number of params may have extra internal params but never less
    Preconditions.checkState(apiQueryBase.getParameters().size() <= numDynamParams);
    return new PagedJdbcQuery(
        query,
        apiQueryBase.getParameters());
  }

  private int countParameters(RelNode relNode) {
    Set<RexDynamicParam> params = new HashSet<>();
    CalciteUtil.applyRexShuttleRecursively(relNode, new RexShuttle(){
      @Override
      public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        params.add(dynamicParam);
        return super.visitDynamicParam(dynamicParam);
      }
    });
    return params.size();
  }

  public static SqlNode convertToSqlNode(RelNode optimizedNode) {
    RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    return converter.visitRoot(optimizedNode).asStatement();
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
}
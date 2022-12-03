
package com.datasqrl.graphql.util;

import com.datasqrl.graphql.server.Model.*;
import com.datasqrl.physical.database.QueryTemplate;
import com.datasqrl.plan.calcite.util.RelToSql;
import com.datasqrl.plan.queries.APIQuery;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ReplaceGraphqlQueries implements
    RootVisitor<Object, Object>,
      CoordVisitor<Object, Object>,
      SchemaVisitor<Object, Object>,
      GraphQLArgumentWrapperVisitor<Object, Object>,
      QueryBaseVisitor<PgQuery, Object>,
      ApiQueryVisitor<PgQuery, Object>,
      ResolvedQueryVisitor<Object, Object>,
      ParameterHandlerVisitor<Object, Object>
  {

    private final Map<APIQuery, QueryTemplate> queries;

    public ReplaceGraphqlQueries(Map<APIQuery, QueryTemplate> queries) {

      this.queries = queries;
    }

    @Override
    public PgQuery visitApiQuery(ApiQueryBase apiQueryBase, Object context) {
      QueryTemplate template = queries.get(apiQueryBase.getQuery());

      SqlWriterConfig config = RelToSql.transform.apply(SqlPrettyWriter.config());
      DynamicParamSqlPrettyWriter writer = new DynamicParamSqlPrettyWriter(config);
      String query = convertDynamicParams(writer, template.getRelNode());
      return PgQuery.builder()
          .parameters(apiQueryBase.getParameters())
          .sql(query)
          .build();
    }

    @Override
    public PgQuery visitPagedApiQuery(PagedApiQueryBase apiQueryBase, Object context) {
      QueryTemplate template = queries.get(apiQueryBase.getQuery());

      //todo builder
      SqlWriterConfig config = RelToSql.transform.apply(SqlPrettyWriter.config());
      DynamicParamSqlPrettyWriter writer = new DynamicParamSqlPrettyWriter(config);
      String query = convertDynamicParams(writer, template.getRelNode());
      Preconditions.checkState(
          Collections.max(writer.dynamicParameters) < apiQueryBase.getParameters().size());
      return new PagedPgQuery(
          query,
          apiQueryBase.getParameters());
    }

    private String convertDynamicParams(DynamicParamSqlPrettyWriter writer, RelNode relNode) {
      SqlNode node = RelToSql.convertToSqlNode(relNode);
      node.unparse(writer, 0, 0);
      return writer.toSqlString().getSql();
    }

    /**
     * Writes postgres style dynamic params `$1` instead of `?`. Assumes the index field is the index
     * of the parameter.
     */
    public class DynamicParamSqlPrettyWriter extends SqlPrettyWriter {

      @Getter
      private List<Integer> dynamicParameters = new ArrayList<>();

      public DynamicParamSqlPrettyWriter(@NonNull SqlWriterConfig config) {
        super(config);
      }

      @Override
      public void dynamicParam(int index) {
        if (dynamicParameters == null) {
          dynamicParameters = new ArrayList<>();
        }
        dynamicParameters.add(index);
        print("$" + (index + 1));
        setNeedWhitespace(true);
      }
    }

    @Override
    public Object visitRoot(RootGraphqlModel root, Object context) {
      root.getCoords().forEach(c->c.accept(this, context));
      return null;
    }

    @Override
    public Object visitStringDefinition(StringSchema stringSchema, Object context) {
      return null;
    }

    @Override
    public Object visitArgumentLookup(ArgumentLookupCoords coords, Object context) {
      coords.getMatchs().forEach(c->{
        PgQuery query = c.getQuery().accept(this, context);
        c.setQuery(query);
      });
      return null;
    }

    @Override
    public Object visitFieldLookup(FieldLookupCoords coords, Object context) {
      return null;
    }

    @Override
    public PgQuery visitPgQuery(PgQuery pgQuery, Object context) {
      return null;
    }

    @Override
    public PgQuery visitPagedPgQuery(PagedPgQuery pgQuery, Object context) {
      return null;
    }

    @Override
    public Object visitSourcePgParameter(SourcePgParameter sourceParameter, Object context) {
      return null;
    }

    @Override
    public Object visitArgumentPgParameter(ArgumentPgParameter argumentParameter, Object context) {
      return null;
    }

    @Override
    public Object visitResolvedPgQuery(ResolvedPgQuery query, Object context) {
      return null;
    }

    @Override
    public Object visitResolvedPagedPgQuery(ResolvedPagedPgQuery query, Object context) {
      return null;
    }

    @Override
    public Object visitArgumentWrapper(GraphQLArgumentWrapper graphQLArgumentWrapper,
        Object context) {
      return null;
    }
  }
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
package com.datasqrl.graphql.server;

import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;

/**
 * Purpose: Encapsulates the GraphQL schema and entry points (queries, mutations, subscriptions).
 * Defines the visitor interfaces to visit the model. This class is deserialized from vertx.json
 * <br>
 * Collaboration:
 *
 * <ul>
 *   <li>the root of the model, the schema, coords, the sql queries, the arguments and parameters
 *       are visited by {@link GraphQLEngineBuilder}
 *   <li>the mutations (kafka and prostgreSQL) are visited by {@link
 *       com.datasqrl.graphql.MutationConfigurationImpl}
 *   <li>the subscriptions (kafka and prostgreSQL) are visited by {@link
 *       com.datasqrl.graphql.SubscriptionConfigurationImpl}
 * </ul>
 */
@Getter
@Builder
public class RootGraphqlModel {

  @Singular List<QueryCoords> queries;
  @Singular List<MutationCoords> mutations;
  @Singular List<SubscriptionCoords> subscriptions;
  @Singular List<ApiOperation> operations;
  Schema schema;

  @JsonCreator
  public RootGraphqlModel(
      @JsonProperty("queries") List<QueryCoords> queries,
      @JsonProperty("mutations") List<MutationCoords> mutations,
      @JsonProperty("subscriptions") List<SubscriptionCoords> subscriptions,
      @JsonProperty("operations") List<ApiOperation> operations,
      @JsonProperty("schema") Schema schema) {
    this.queries = queries;
    this.mutations = mutations == null ? List.of() : mutations;
    this.subscriptions = subscriptions == null ? List.of() : subscriptions;
    this.operations = operations == null ? List.of() : operations;
    this.schema = schema;
  }

  public <R, C> R accept(RootVisitor<R, C> visitor, C context) {
    return visitor.visitRoot(this, context);
  }

  public interface RootVisitor<R, C> {

    R visitRoot(RootGraphqlModel root, C context);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({@Type(value = StringSchema.class, name = "string")})
  public interface Schema {

    <R, C> R accept(SchemaVisitor<R, C> visitor, C context);
  }

  public interface SchemaVisitor<R, C> {

    R visitStringDefinition(StringSchema stringSchema, C context);
  }

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class StringSchema implements Schema {

    String schema;

    @Override
    public <R, C> R accept(SchemaVisitor<R, C> visitor, C context) {
      return visitor.visitStringDefinition(this, context);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({@Type(value = KafkaMutationCoords.class, name = KafkaMutationCoords.type)})
  public abstract static class MutationCoords {
    protected String type;

    public abstract <R, C> R accept(MutationCoordsVisitor<R, C> visitor, C context);

    public abstract String getFieldName();

    public abstract boolean isReturnList();
  }

  public interface MutationCoordsVisitor<R, C> {
    R visit(KafkaMutationCoords coords, C context);
  }

  @Getter
  @NoArgsConstructor
  public static class KafkaMutationCoords extends MutationCoords {

    private static final String type = "kafka";

    protected String fieldName;
    protected boolean returnList;
    protected String topic;
    protected Map<String, MutationComputedColumnType> computedColumns;
    protected boolean transactional;
    protected Map<String, String> sinkConfig;

    public KafkaMutationCoords(
        String fieldName,
        boolean returnList,
        String topic,
        Map<String, MutationComputedColumnType> computedColumns,
        boolean transactional,
        Map<String, String> sinkConfig) {
      this.fieldName = fieldName;
      this.returnList = returnList;
      this.topic = topic;
      this.computedColumns = computedColumns;
      this.transactional = transactional;
      this.sinkConfig = sinkConfig;
    }

    @Override
    public <R, C> R accept(MutationCoordsVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "type",
      defaultImpl = KafkaSubscriptionCoords.class)
  @JsonSubTypes({
    @Type(value = KafkaSubscriptionCoords.class, name = "kafka"),
    @Type(value = PostgresSubscriptionCoords.class, name = "postgres_log")
  })
  public abstract static class SubscriptionCoords {
    protected String type;

    public abstract <R, C> R accept(SubscriptionCoordsVisitor<R, C> visitor, C context);

    public abstract String getFieldName();
  }

  public interface SubscriptionCoordsVisitor<R, C> {
    R visit(KafkaSubscriptionCoords coords, C context);

    R visit(PostgresSubscriptionCoords coords, C context);
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder
  public static class KafkaSubscriptionCoords extends SubscriptionCoords {

    private static final String type = "kafka";

    protected String fieldName;
    protected String topic;
    protected Map<String, String> sinkConfig;
    protected Map<String, String> filters;

    @Override
    public <R, C> R accept(SubscriptionCoordsVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class PostgresSubscriptionCoords extends SubscriptionCoords {

    private static final String type = "postgres_log";

    protected String fieldName;
    protected String tableName;
    protected Map<String, String> filters;
    protected String listenQuery;
    protected String onNotifyQuery;
    protected List<String> parameters;

    @Override
    public <R, C> R accept(SubscriptionCoordsVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  public interface QueryCoordVisitor<R, C> {

    R visitArgumentLookup(ArgumentLookupQueryCoords coords, C context);

    R visitFieldLookup(FieldLookupQueryCoords coords, C context);
  }

  /**
   * Binds graphql queries with corresponding sql queries and the combinations of their arguments:
   * out of graphQL schema we generate all possible SQL queries for the possible arguments.
   * (nullable, not nullable, same table called with different sets of args). These combinations are
   * matchs which contain arguments, sql query and parameters.
   */
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @Type(value = ArgumentLookupQueryCoords.class, name = "args"),
    @Type(value = FieldLookupQueryCoords.class, name = "field")
  })
  public abstract static class QueryCoords { // TODO should be renamed QueryCoords

    String parentType;
    String fieldName;

    public abstract <R, C> R accept(QueryCoordVisitor<R, C> visitor, C context);
  }

  @Getter
  @NoArgsConstructor
  public static class FieldLookupQueryCoords extends QueryCoords {

    String columnName;

    @Builder
    public FieldLookupQueryCoords(String parentType, String fieldName, String columnName) {
      super(parentType, fieldName);
      this.columnName = columnName;
    }

    @Override
    public <R, C> R accept(QueryCoordVisitor<R, C> visitor, C context) {
      return visitor.visitFieldLookup(this, context);
    }
  }

  @Getter
  @NoArgsConstructor
  public static class ArgumentLookupQueryCoords extends QueryCoords {

    /** The executable query with the query base and GraphQL Arguments */
    QueryWithArguments exec;

    @Builder
    public ArgumentLookupQueryCoords(String parentType, String fieldName, QueryWithArguments exec) {
      super(parentType, fieldName);
      this.exec = exec;
    }

    @Override
    public <R, C> R accept(QueryCoordVisitor<R, C> visitor, C context) {
      return visitor.visitArgumentLookup(this, context);
    }
  }

  @Builder
  @Getter
  @Setter
  @AllArgsConstructor
  @NoArgsConstructor
  @ToString
  public static class QueryWithArguments {

    // May be empty for no-args
    @Singular Set<Argument> arguments;
    QueryBase query;
  }

  public interface QueryBaseVisitor<R, C> {

    R visitSqlQuery(SqlQuery sqlQuery, C context);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({@Type(value = SqlQuery.class, name = "SqlQuery")})
  public interface QueryBase {

    <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context);
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class SqlQuery implements QueryBase {

    /** The SQL query to execute */
    String sql;

    /** The list of parameters to substitute at query time */
    @Singular List<QueryParameterHandler> parameters;

    /** How query results are paginated */
    PaginationType pagination;

    /** For how long the results of this query can be cached. 0 to disable caching */
    long cacheTime;

    /** The database the query is executed against */
    DatabaseType database;

    public boolean requiresDynamicLimitOffset() {
      return pagination == PaginationType.LIMIT_AND_OFFSET && !database.supportsLimitOffsetBinding;
    }

    @Override
    public <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context) {
      return visitor.visitSqlQuery(this, context);
    }

    public SqlQuery updateSQL(String newSQL) {
      return new SqlQuery(newSQL, parameters, pagination, cacheTime, database);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @Type(value = FixedArgument.class, name = FixedArgument.type),
    @Type(value = VariableArgument.class, name = VariableArgument.type)
  })
  public interface Argument {

    String getPath();

    Object getValue();
  }

  public interface VariableArgumentVisitor<R, C> {

    R visitVariableArgument(VariableArgument variableArgument, C context);
  }

  /** A variable argument */
  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class VariableArgument implements Argument {

    static final String type = "variable";
    String path;
    Object value;

    public <R, C> R accept(VariableArgumentVisitor<R, C> visitor, C context) {
      return visitor.visitVariableArgument(this, context);
    }

    // Exclude the value for variable arguments
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      var that = (VariableArgument) o;
      return Objects.equals(type, VariableArgument.type) && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, path);
    }

    @Override
    public String toString() {
      return "VariableArgument{" + "path='" + path + '\'' + '}';
    }
  }

  public interface FixedArgumentVisitor<R, C> {

    R visitFixedArgument(FixedArgument fixedArgument, C context);
  }

  /** An argument with a scalar value */
  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @ToString
  public static class FixedArgument implements Argument {

    static final String type = "fixed";

    String path;
    Object value;

    public <R, C> R accept(FixedArgumentVisitor<R, C> visitor, C context) {
      return visitor.visitFixedArgument(this, context);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @Type(value = ParentParameter.class, name = ParentParameter.type),
    @Type(value = ArgumentParameter.class, name = ArgumentParameter.type),
    @Type(value = MetadataParameter.class, name = MetadataParameter.type)
  })
  public interface QueryParameterHandler {

    <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context);
  }

  public interface ParameterHandlerVisitor<R, C> {

    R visitParentParameter(ParentParameter parentParameter, C context);

    R visitMetadataParameter(MetadataParameter metadataParameter, C context);

    R visitArgumentParameter(ArgumentParameter argumentParameter, C context);
  }

  /** Parameter is passed in from the parent table that a relationship is defined on. */
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder
  @ToString
  public static class ParentParameter implements QueryParameterHandler {

    static final String type = "source";
    String key;

    @Override
    public <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context) {
      return visitor.visitParentParameter(this, context);
    }
  }

  /** Paramter is an argument provided by the user */
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder
  @ToString
  public static class ArgumentParameter implements QueryParameterHandler {

    static final String type = "arg";
    String path;

    @Override
    public <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context) {
      return visitor.visitArgumentParameter(this, context);
    }
  }

  /** Parameter is metadata that is extracted from the context (e.g. auth) */
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @ToString
  public static class MetadataParameter implements QueryParameterHandler {

    static final String type = "metadata";
    ResolvedMetadata metadata;

    @Override
    public <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context) {
      return visitor.visitMetadataParameter(this, context);
    }
  }

  public interface ResolvedQueryVisitor<R, C> {

    R visitResolvedSqlQuery(ResolvedSqlQuery query, C context);
  }

  public interface ResolvedQuery {

    QueryBase getQuery();

    public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context);
  }

  @AllArgsConstructor
  @Getter
  @NoArgsConstructor
  public static class ResolvedSqlQuery implements ResolvedQuery {

    SqlQuery query;
    PreparedSqrlQuery preparedQueryContainer;

    @Override
    public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context) {
      return visitor.visitResolvedSqlQuery(this, context);
    }
  }

  public interface PreparedSqrlQuery<T> {

    T preparedQuery();
  }
}

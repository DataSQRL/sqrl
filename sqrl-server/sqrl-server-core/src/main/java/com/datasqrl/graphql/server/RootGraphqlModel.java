/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.server;

import com.datasqrl.graphql.jdbc.DatabaseType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
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
 *   <li>the root of the model, the schema, coords, the jdbc queries, the arguments and parameters
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

  Schema schema;

  @JsonCreator
  public RootGraphqlModel(
      @JsonProperty("queries") List<QueryCoords> queries,
      @JsonProperty("mutations") List<MutationCoords> mutations,
      @JsonProperty("subscriptions") List<SubscriptionCoords> subscriptions,
      @JsonProperty("schema") Schema schema) {
    this.queries = queries;
    this.mutations = mutations == null ? List.of() : mutations;
    this.subscriptions = subscriptions == null ? List.of() : subscriptions;
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

    final String type = "string";
    String schema;

    public <R, C> R accept(SchemaVisitor<R, C> visitor, C context) {
      return visitor.visitStringDefinition(this, context);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @Type(value = KafkaMutationCoords.class, name = KafkaMutationCoords.type),
    @Type(value = PostgresLogMutationCoords.class, name = PostgresLogMutationCoords.type)
  })
  public abstract static class MutationCoords {
    protected String type;

    public abstract <R, C> R accept(MutationCoordsVisitor<R, C> visitor, C context);

    public abstract String getFieldName();
  }

  public interface MutationCoordsVisitor<R, C> {
    R visit(KafkaMutationCoords coords, C context);

    R visit(PostgresLogMutationCoords coords, C c);
  }

  @Getter
  @NoArgsConstructor
  public static class KafkaMutationCoords extends MutationCoords {

    private static final String type = "kafka";

    protected String fieldName;
    protected String topic;
    protected Map<String, MutationComputedColumnType> computedColumns;
    protected Map<String, String> sinkConfig;

    public KafkaMutationCoords(
        String fieldName,
        String topic,
        Map<String, MutationComputedColumnType> computedColumns,
        Map<String, String> sinkConfig) {
      this.fieldName = fieldName;
      this.topic = topic;
      this.computedColumns = computedColumns;
      this.sinkConfig = sinkConfig;
    }

    @Override
    public <R, C> R accept(MutationCoordsVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @Getter
  @NoArgsConstructor
  public static class PostgresLogMutationCoords extends MutationCoords {

    private static final String type = "postgres_log";

    protected String fieldName;
    protected String tableName;
    protected String insertStatement;
    protected List<String> parameters;

    public PostgresLogMutationCoords(
        String fieldName, String tableName, String insertStatement, List<String> parameters) {
      this.fieldName = fieldName;
      this.tableName = tableName;
      this.insertStatement = insertStatement;
      this.parameters = parameters;
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

    @JsonIgnore final String type = "field";
    String columnName;

    @Builder
    public FieldLookupQueryCoords(String parentType, String fieldName, String columnName) {
      super(parentType, fieldName);
      this.columnName = columnName;
    }

    public <R, C> R accept(QueryCoordVisitor<R, C> visitor, C context) {
      return visitor.visitFieldLookup(this, context);
    }
  }

  @Getter
  @NoArgsConstructor
  public static class ArgumentLookupQueryCoords extends QueryCoords {

    @JsonIgnore final String type = "args";

    /**
     * The executable query with the query base and GraphQL Arguments
     */
    QueryWithArguments exec;

    @Builder
    public ArgumentLookupQueryCoords(
        String parentType, String fieldName, QueryWithArguments exec) {
      super(parentType, fieldName);
      this.exec = exec;
    }

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
  @JsonSubTypes({
    @Type(value = SqlQuery.class, name = "JdbcQuery")
  })
  public interface QueryBase {

    <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context);
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class SqlQuery implements QueryBase {

    final String type = "JdbcQuery";
    String sql;
    @Singular List<JdbcParameterHandler> parameters;
    PaginationType pagination;
    DatabaseType database;

    public boolean requiresDynamicLimitOffset() {
      return pagination==PaginationType.LIMIT_AND_OFFSET && !database.supportsLimitOffsetBinding;
    }

    @Override
    public <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context) {
      return visitor.visitSqlQuery(this, context);
    }

    public SqlQuery updateSQL(String newSQL) {
      return new SqlQuery(newSQL, parameters, pagination, database);
    }

  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @Type(value = FixedArgument.class, name = "fixed"),
    @Type(value = VariableArgument.class, name = "variable")
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

    final String type = "variable";
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
      VariableArgument that = (VariableArgument) o;
      return Objects.equals(type, that.type) && Objects.equals(path, that.path);
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

    final String type = "fixed";

    String path;
    Object value;

    public <R, C> R accept(FixedArgumentVisitor<R, C> visitor, C context) {
      return visitor.visitFixedArgument(this, context);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @Type(value = SourceParameter.class, name = "source"),
    @Type(value = ArgumentParameter.class, name = "arg")
  })
  public interface JdbcParameterHandler {

    <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context);
  }

  public interface ParameterHandlerVisitor<R, C> {

    R visitSourceParameter(SourceParameter sourceParameter, C context);

    R visitArgumentParameter(ArgumentParameter argumentParameter, C context);
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder
  @ToString
  public static class SourceParameter implements JdbcParameterHandler {

    final String type = "source";
    String key;

    public <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context) {
      return visitor.visitSourceParameter(this, context);
    }
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder
  @ToString
  public static class ArgumentParameter implements JdbcParameterHandler {

    final String type = "arg";
    String path;

    public <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context) {
      return visitor.visitArgumentParameter(this, context);
    }
  }

  public interface ResolvedQueryVisitor<R, C> {

    R visitResolvedJdbcQuery(ResolvedJdbcQuery query, C context);
  }

  public interface ResolvedQuery {

    QueryBase getQuery();

    public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context);
  }

  @AllArgsConstructor
  @Getter
  @NoArgsConstructor
  public static class ResolvedJdbcQuery implements ResolvedQuery {

    SqlQuery query;
    PreparedSqrlQuery preparedQueryContainer;

    @Override
    public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context) {
      return visitor.visitResolvedJdbcQuery(this, context);
    }
  }

  public interface PreparedSqrlQuery<T> {

    T getPreparedQuery();
  }

}

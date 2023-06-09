/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.server;

import com.datasqrl.config.SerializedSqrlConfig;
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

public class Model {

  public interface RootVisitor<R, C> {

    R visitRoot(RootGraphqlModel root, C context);
  }

  @Getter
  @Builder
  @NoArgsConstructor
  public static class RootGraphqlModel {

    @Singular
    List<QueryCoords> coords;
    @Singular
    List<MutationCoords> mutations;
    @Singular
    List<SubscriptionCoords> subscriptions;

    Schema schema;

    @JsonCreator
    public RootGraphqlModel(
        @JsonProperty("coords") List<QueryCoords> coords,
        @JsonProperty("mutations") List<MutationCoords> mutations,
        @JsonProperty("subscriptions") List<SubscriptionCoords> subscriptions,
        @JsonProperty("schema") Schema schema) {
      this.coords = coords;
      this.mutations = mutations == null ? List.of() : mutations;
      this.subscriptions = subscriptions == null ? List.of() : subscriptions;
      this.schema = schema;
    }

    public <R, C> R accept(RootVisitor<R, C> visitor, C context) {
      return visitor.visitRoot(this, context);
    }
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.PROPERTY,
      property = "type")
  @JsonSubTypes({
      @Type(value = StringSchema.class, name = "string")
  })
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

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class MutationCoords {
    protected String fieldName;
    protected SerializedSqrlConfig sinkConfig;
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class SubscriptionCoords {
    protected String fieldName;
    protected SerializedSqrlConfig sinkConfig;
  }


  public interface CoordVisitor<R, C> {

    R visitArgumentLookup(ArgumentLookupQueryCoords coords, C context);

    R visitFieldLookup(FieldLookupQueryCoords coords, C context);
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.PROPERTY,
      property = "type")
  @JsonSubTypes({
      @Type(value = ArgumentLookupQueryCoords.class, name = "args"),
      @Type(value = FieldLookupQueryCoords.class, name = "field")
  })
  public static abstract class QueryCoords {

    String parentType;
    String fieldName;

    public abstract <R, C> R accept(CoordVisitor<R, C> visitor, C context);
  }

  @Getter
  @NoArgsConstructor
  public static class FieldLookupQueryCoords extends QueryCoords {

    @JsonIgnore
    final String type = "field";
    String columnName;

    @Builder
    public FieldLookupQueryCoords(String parentType, String fieldName,
        String columnName) {
      super(parentType, fieldName);
      this.columnName = columnName;
    }

    public <R, C> R accept(CoordVisitor<R, C> visitor, C context) {
      return visitor.visitFieldLookup(this, context);
    }
  }

  @Getter
  @NoArgsConstructor
  public static class ArgumentLookupQueryCoords extends QueryCoords {

    @JsonIgnore
    final String type = "args";
    Set<ArgumentSet> matchs;

    @Builder
    public ArgumentLookupQueryCoords(String parentType, String fieldName,
        @Singular Set<ArgumentSet> matchs) {
      super(parentType, fieldName);
      this.matchs = matchs;
    }

    public <R, C> R accept(CoordVisitor<R, C> visitor, C context) {
      return visitor.visitArgumentLookup(this, context);
    }
  }

  @Builder
  @Getter
  @Setter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class ArgumentSet {

    //The may be empty for no-args
    @Singular
    Set<Argument> arguments;
    QueryBase query;
  }

  public interface QueryBaseVisitor<R, C> {

    R visitJdbcQuery(JdbcQuery jdbcQuery, C context);

    R visitPagedJdbcQuery(PagedJdbcQuery jdbcQuery, C context);
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.PROPERTY,
      property = "type")
  @JsonSubTypes({
      @Type(value = JdbcQuery.class, name = "JdbcQuery"),
      @Type(value = PagedJdbcQuery.class, name = "PagedJdbcQuery")
  })
  public interface QueryBase {

    <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context);
  }

  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class JdbcQuery implements QueryBase {

    final String type = "JdbcQuery";
    String sql;
    @Singular
    List<JdbcParameterHandler> parameters;

    @Override
    public <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context) {
      return visitor.visitJdbcQuery(this, context);
    }
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class PagedJdbcQuery extends JdbcQuery {

    final String type = "PagedJdbcQuery";
    String sql;
    @Singular
    List<JdbcParameterHandler> parameters;

    @Override
    public <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context) {
      return visitor.visitPagedJdbcQuery(this, context);
    }
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.PROPERTY,
      property = "type")
  @JsonSubTypes({
      @Type(value = FixedArgument.class, name = "fixed"),
      @Type(value = VariableArgument.class, name = "variable")
  })
  public interface Argument {

    String getPath();
  }


  public interface VariableArgumentVisitor<R, C> {

    R visitVariableArgument(VariableArgument variableArgument, C context);
  }

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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o instanceof Argument) {
        Argument that = (Argument) o;
        return Objects.equals(path, that.getPath());
      }

      return false;
    }

    //non-standard hash code, hash on 'path' so it can be compared with an ArgumentVariable
    @Override
    public int hashCode() {
      return Objects.hash(path);
    }

    @Override
    public String toString() {
      return "VariableArgument{" +
          "path='" + path + '\'' +
          '}';
    }
  }

  public interface FixedArgumentVisitor<R, C> {

    R visitFixedArgument(FixedArgument fixedArgument, C context);
  }

  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class FixedArgument implements Argument {

    final String type = "fixed";

    String path;
    Object value;

    public <R, C> R accept(FixedArgumentVisitor<R, C> visitor, C context) {
      return visitor.visitFixedArgument(this, context);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o instanceof VariableArgument) {
        VariableArgument that = (VariableArgument) o;
        return Objects.equals(path, that.path);
      } else if (o instanceof FixedArgument) {
        FixedArgument that = (FixedArgument) o;
        return Objects.equals(path, that.path) && Objects.equals(value, that.value);
      }

      return false;
    }

    //non-standard hash code, hash on 'path' so it can be compared with an ArgumentVariable
    @Override
    public int hashCode() {
      return Objects.hash(path);
    }
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.PROPERTY,
      property = "type")
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
  public static class ArgumentParameter implements JdbcParameterHandler {

    final String type = "arg";
    String path;

    public <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context) {
      return visitor.visitArgumentParameter(this, context);
    }
  }

  public interface ResolvedQueryVisitor<R, C> {

    public R visitResolvedJdbcQuery(ResolvedJdbcQuery query, C context);

    public R visitResolvedPagedJdbcQuery(ResolvedPagedJdbcQuery query, C context);
  }

  public interface ResolvedQuery {

    public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context);
  }

  @AllArgsConstructor
  @Getter
  @NoArgsConstructor
  public static class ResolvedJdbcQuery implements ResolvedQuery {

    JdbcQuery query;
    PreparedSqrlQuery preparedQueryContainer;

    @Override
    public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context) {
      return visitor.visitResolvedJdbcQuery(this, context);
    }
  }

  public interface PreparedSqrlQuery<T> {

    T getPreparedQuery();
  }

  @AllArgsConstructor
  @Getter
  @NoArgsConstructor
  public static class ResolvedPagedJdbcQuery implements ResolvedQuery {

    PagedJdbcQuery query;

    @Override
    public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context) {
      return visitor.visitResolvedPagedJdbcQuery(this, context);
    }
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class GraphQLArgumentWrapper {

    Map<String, Object> args;

    public static GraphQLArgumentWrapper wrap(Map<String, Object> args) {
      return new GraphQLArgumentWrapper(args);
    }

    public <R, C> R accept(GraphQLArgumentWrapperVisitor<R, C> visitor, C context) {
      return visitor.visitArgumentWrapper(this, context);
    }
  }

  public interface GraphQLArgumentWrapperVisitor<R, C> {

    R visitArgumentWrapper(GraphQLArgumentWrapper graphQLArgumentWrapper, C context);
  }
}

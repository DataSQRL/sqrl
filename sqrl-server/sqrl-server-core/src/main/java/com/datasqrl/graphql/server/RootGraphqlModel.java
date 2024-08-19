/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.server;

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

@Getter
@Builder
public class RootGraphqlModel {

  @Singular
  List<Coords> coords;
  @Singular
  List<MutationCoords> mutations;
  @Singular
  List<SubscriptionCoords> subscriptions;

  Schema schema;

  @JsonCreator
  public RootGraphqlModel(
      @JsonProperty("coords") List<Coords> coords,
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

  public interface RootVisitor<R, C> {

    R visitRoot(RootGraphqlModel root, C context);
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
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
    protected String topic;
    protected Map<String, String> sinkConfig;
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder
  public static class SubscriptionCoords {
    protected String fieldName;
    protected String topic;
    protected Map<String, String> sinkConfig;
    protected Map<String, String> filters;
  }

  public interface CoordVisitor<R, C> {

    R visitArgumentLookup(ArgumentLookupCoords coords, C context);

    R visitFieldLookup(FieldLookupCoords coords, C context);
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "type")
  @JsonSubTypes({
      @Type(value = ArgumentLookupCoords.class, name = "args"),
      @Type(value = FieldLookupCoords.class, name = "field")
  })
  public static abstract class Coords {

    String parentType;
    String fieldName;

    public abstract <R, C> R accept(CoordVisitor<R, C> visitor, C context);
  }

  @Getter
  @NoArgsConstructor
  public static class FieldLookupCoords extends Coords {

    @JsonIgnore
    final String type = "field";
    String columnName;

    @Builder
    public FieldLookupCoords(String parentType, String fieldName,
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
  public static class ArgumentLookupCoords extends Coords {

    @JsonIgnore
    final String type = "args";
    Set<ArgumentSet> matchs;

    @Builder
    public ArgumentLookupCoords(String parentType, String fieldName,
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
  @ToString
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
      property = "type")
  @JsonSubTypes({
      @Type(value = JdbcQuery.class, name = "JdbcQuery"),
      @Type(value = PagedJdbcQuery.class, name = "PagedJdbcQuery")
  })
  public interface QueryBase {

    <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context);
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class JdbcQuery implements QueryBase {

    final String type = "JdbcQuery";
    String database = "postgres";
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
    String database = "postgres";
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
      property = "type")
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

  /**
   * A variable argument
   */
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

    //Exclude the value for variable arguments
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
      return "VariableArgument{" +
          "path='" + path + '\'' +
          '}';
    }
  }

  public interface FixedArgumentVisitor<R, C> {

    R visitFixedArgument(FixedArgument fixedArgument, C context);
  }

  /**
   * An argument with a scalar value
   */
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

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
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
    String database = "postgres";

    PagedJdbcQuery query;

    @Override
    public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context) {
      return visitor.visitResolvedPagedJdbcQuery(this, context);
    }
  }
}

package ai.datasqrl.graphql.server;

import graphql.schema.idl.TypeDefinitionRegistry;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Singular;

public class Model {
  interface RootVisitor<R, C> {
    R visitRoot(Root root, C context);
  }

  @Getter
  @Builder
  public static class Root {
    @Singular
    List<Coords> coords;
    Schema schema;

    public <R, C> R accept(RootVisitor<R, C> visitor, C context) {
      return visitor.visitRoot(this, context);
    }
  }

  public interface Schema {
    <R, C> R accept(SchemaVisitor<R, C> visitor, C context);
  }

  interface SchemaVisitor<R, C> {
    R visitTypeDefinition(TypeDefinitionSchema typeDefinitionSchema, C context);
    R visitStringDefinition(StringSchema stringSchema, C context);
  }

  @Builder
  @Getter
  public static class StringSchema implements Schema {
    String schema;
    public <R, C> R accept(SchemaVisitor<R, C> visitor, C context) {
      return visitor.visitStringDefinition(this, context);
    }
  }

  @Builder
  @Getter
  public static class TypeDefinitionSchema implements Schema {
    TypeDefinitionRegistry typeDefinitionRegistry;
    public <R, C> R accept(SchemaVisitor<R, C> visitor, C context) {
      return visitor.visitTypeDefinition(this, context);
    }
  }

  interface CoordVisitor<R, C> {
    R visitArgumentLookup(ArgumentLookupCoords coords, C context);
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static abstract class Coords {
    String parentType;
    String fieldName;
    public <R, C> R accept(CoordVisitor<R, C> visitor, C context) {
      return null;
    }
  }

  @Getter
  public static class ArgumentLookupCoords extends Coords {
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
  public static class ArgumentSet {
    //The may be empty for no-args
    @Singular
    Set<Argument> arguments;
    QueryBase query;
  }

  public interface QueryBaseVisitor<R,C> {
    R visitPgQuery(PgQuery pgQuery, C context);
  }

  public interface QueryBase {
    <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context);
  }

  @Builder
  @Getter
  public static class PgQuery implements QueryBase {
    String sql;
    @Singular
    List<PgParameterHandler> parameters;

    @Override
    public <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context) {
      return visitor.visitPgQuery(this, context);
    }
  }

  public interface Argument {
    String getPath();
  }


  public interface VariableArgumentVisitor<R, C> {
    R visitVariableArgument(VariableArgument variableArgument, C context);
  }

  @Builder
  @Getter
  public static class VariableArgument implements Argument {
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
  }

  public interface FixedArgumentVisitor<R, C> {
    R visitFixedArgument(FixedArgument fixedArgument, C context);
  }

  @Builder
  @Getter
  public static class FixedArgument implements Argument {
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

  public interface PgParameterHandler {
    <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context);
  }

  public interface ParameterHandlerVisitor<R, C> {
    R visitSourcePgParameter(SourcePgParameter sourceParameter, C context);
    R visitArgumentPgParameter(ArgumentPgParameter argumentParameter, C context);
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder
  public static class SourcePgParameter implements PgParameterHandler {
    String key;
    public <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context) {
      return visitor.visitSourcePgParameter(this, context);
    }
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder
  public static class ArgumentPgParameter implements PgParameterHandler {
    String path;
    public <R, C> R accept(ParameterHandlerVisitor<R, C> visitor, C context) {
      return visitor.visitArgumentPgParameter(this, context);
    }
  }

  public interface ResolvedQueryVisitor<R, C> {
    public R visitResolvedPgQuery(ResolvedPgQuery query, C context);
  }

  public interface ResolvedQuery {
    public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context);
  }

  @AllArgsConstructor
  @Getter
  public static class ResolvedPgQuery implements ResolvedQuery {
    PgQuery query;
    PreparedQuery<RowSet<Row>> preparedQuery;

    @Override
    public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context) {
      return visitor.visitResolvedPgQuery(this, context);
    }
  }

  @Getter
  @AllArgsConstructor
  public static class GraphQLArgumentWrapper {

    Map<String, Object> args;

    public static GraphQLArgumentWrapper wrap(Map<String, Object> args) {
      return new GraphQLArgumentWrapper(args);
    }

    public <R, C> R accept(GraphQLArgumentWrapperVisitor<R, C> visitor, C context) {
      return visitor.visitArgumentWrapper(this, context);
    }
  }

  public interface GraphQLArgumentWrapperVisitor <R, C> {
    R visitArgumentWrapper(GraphQLArgumentWrapper graphQLArgumentWrapper, C context);
  }
}

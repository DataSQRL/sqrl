package ai.dataeng.sqml;

import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.ArraySqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.DateTimeSqmlType;
import ai.dataeng.sqml.type.SqmlType.NullSqmlType;
import ai.dataeng.sqml.type.SqmlType.NumberSqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import ai.dataeng.sqml.type.SqmlType.UnknownSqmlType;
import ai.dataeng.sqml.type.SqmlType.UuidSqmlType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import graphql.schema.GraphQLArgument;
import java.util.List;

public class GqlTypeArgumentVisitor extends SqmlTypeVisitor<List<GraphQLArgument>, Object> {

  @Override
  public List<GraphQLArgument> visitSqmlType(SqmlType type, Object context) {
    throw new RuntimeException(String.format("Could not find type %s", type));
  }

  @Override
  public List<GraphQLArgument> visitUuid(UuidSqmlType type, Object context) {
    return List.of();
  }

  @Override
  public List<GraphQLArgument> visitUnknown(UnknownSqmlType type, Object context) {
    return List.of();
  }

  @Override
  public List<GraphQLArgument> visitDateTime(DateTimeSqmlType type, Object context) {
    return List.of();
  }

  @Override
  public List<GraphQLArgument> visitNull(NullSqmlType type, Object context) {
    return List.of();
  }

  @Override
  public List<GraphQLArgument> visitArray(ArraySqmlType type, Object context) {
    return List.of();
  }

  @Override
  public List<GraphQLArgument> visitNumber(NumberSqmlType numberSqmlType, Object context) {
    return List.of();
  }

  @Override
  public List<GraphQLArgument> visitString(StringSqmlType type, Object context) {
    return List.of();
  }

  @Override
  public List<GraphQLArgument> visitBoolean(BooleanSqmlType type, Object context) {
    return List.of();
  }

  @Override
  public List<GraphQLArgument> visitRelation(RelationSqmlType type, Object context) {
    return List.of();
  }
}

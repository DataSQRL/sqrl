package ai.dataeng.sqml;

import ai.dataeng.sqml.type.SqmlType.ArraySqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.FloatSqmlType;
import ai.dataeng.sqml.type.SqmlType.IntegerSqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLOutputType;
import java.util.List;

public class GqlTypeArgumentVisitor extends SqmlTypeVisitor<List<GraphQLArgument>, Object> {

  @Override
  public List<GraphQLArgument> visitArray(ArraySqmlType type, Object context) {
    return List.of();
  }

  @Override
  public List<GraphQLArgument> visitFloat(FloatSqmlType type, Object context) {
    return List.of();
  }

  @Override
  public List<GraphQLArgument> visitInteger(IntegerSqmlType type, Object context) {
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
}

package ai.dataeng.sqml;

import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.ArraySqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.FloatSqmlType;
import ai.dataeng.sqml.type.SqmlType.IntegerSqmlType;
import ai.dataeng.sqml.type.SqmlType.NumberSqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import graphql.Scalars;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;

public class GqlTypeVisitor extends SqmlTypeVisitor<GraphQLOutputType, Object> {

  @Override
  public GraphQLOutputType visit(ArraySqmlType type, Object context) {
    return GraphQLList.list(type.getSubType().accept(this, context));
  }

  @Override
  public GraphQLOutputType visit(FloatSqmlType type, Object context) {
    return Scalars.GraphQLFloat;
  }

  @Override
  public GraphQLOutputType visit(IntegerSqmlType type, Object context) {
    return Scalars.GraphQLInt;
  }

  @Override
  public GraphQLOutputType visit(StringSqmlType type, Object context) {
    return Scalars.GraphQLString;
  }

  @Override
  public GraphQLOutputType visit(BooleanSqmlType type, Object context) {
    return Scalars.GraphQLBoolean;
  }
}

package ai.dataeng.sqml;

import static ai.dataeng.sqml.GraphqlSchemaBuilder.Visitor.toName;

import ai.dataeng.sqml.GraphqlSchemaBuilder.Context;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.ArraySqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.DateTimeSqmlType;
import ai.dataeng.sqml.type.SqmlType.FloatSqmlType;
import ai.dataeng.sqml.type.SqmlType.IntegerSqmlType;
import ai.dataeng.sqml.type.SqmlType.NullSqmlType;
import ai.dataeng.sqml.type.SqmlType.NumberSqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import ai.dataeng.sqml.type.SqmlType.UnknownSqmlType;
import ai.dataeng.sqml.type.SqmlType.UuidSqmlType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import graphql.Scalars;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLTypeReference;

public class GqlTypeVisitor extends SqmlTypeVisitor<GraphQLOutputType, Context> {

  @Override
  public GraphQLOutputType visitArray(ArraySqmlType type, Context context) {
    return GraphQLList.list(type.getSubType().accept(this, context));
  }

  @Override
  public GraphQLOutputType visitNumber(NumberSqmlType type, Context context) {
    return Scalars.GraphQLFloat;
  }

  @Override
  public GraphQLOutputType visitUnknown(UnknownSqmlType type, Context context) {
    return Scalars.GraphQLString;
  }

  @Override
  public GraphQLOutputType visitDateTime(DateTimeSqmlType type, Context context) {
    return Scalars.GraphQLString;
  }

  @Override
  public GraphQLOutputType visitNull(NullSqmlType type, Context context) {
    return Scalars.GraphQLString;
  }

  @Override
  public GraphQLOutputType visitString(StringSqmlType type, Context context) {
    return Scalars.GraphQLString;
  }

  @Override
  public GraphQLOutputType visitBoolean(BooleanSqmlType type, Context context) {
    return Scalars.GraphQLBoolean;
  }

  @Override
  public GraphQLOutputType visitSqmlType(SqmlType type, Context context) {
    throw new RuntimeException(String.format("Could not find type: ", type));
  }

  @Override
  public GraphQLOutputType visitScalarType(SqmlType type, Context context) {
    return super.visitScalarType(type, context);
  }

  @Override
  public GraphQLOutputType visitRelation(RelationSqmlType type, Context context) {
    return GraphQLList.list(new GraphQLTypeReference(toName(type.getRelationName())));
  }

  @Override
  public GraphQLOutputType visitUuid(UuidSqmlType type, Context context) {
    return Scalars.GraphQLString;
  }

  @Override
  public GraphQLOutputType visitFloat(FloatSqmlType type, Context context) {
    return Scalars.GraphQLFloat;
  }

  @Override
  public GraphQLOutputType visitInteger(IntegerSqmlType type, Context context) {
    return Scalars.GraphQLInt;
  }
}

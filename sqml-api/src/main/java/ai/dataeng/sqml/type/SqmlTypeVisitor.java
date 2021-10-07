package ai.dataeng.sqml.type;


import ai.dataeng.sqml.logical.DistinctRelationDefinition;
import ai.dataeng.sqml.logical.ExtendedChildQueryRelationDefinition;
import ai.dataeng.sqml.logical.ExtendedChildRelationDefinition;
import ai.dataeng.sqml.logical.ExtendedFieldRelationDefinition;
import ai.dataeng.sqml.logical.ImportRelationDefinition;
import ai.dataeng.sqml.logical.QueryRelationDefinition;
import ai.dataeng.sqml.logical.RelationDefinition;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;

public class SqmlTypeVisitor<R, C> {
//  public R visitSqmlType(Type type, C context) {
//    return null;
//  }
//  public R visitArray(ArrayType type, C context) {
//    return visitSqmlType(type, context);
//  }
//  public R visitScalarType(Type type, C context) {
//    return visitSqmlType(type, context);
//  }
//  public R visitRelation(RelationType type, C context) {
//    return visitSqmlType(type, context);
//  }
//  public R visitString(StringType type, C context) {
//    return visitScalarType(type, context);
//  }
//  public R visitBoolean(BooleanType type, C context) {
//    return visitScalarType(type, context);
//  }
//  public R visitNumber(NumberType type, C context) {
//    return visitScalarType(type, context);
//  }
//  public R visitUnknown(UnknownType type, C context) {
//    return visitScalarType(type, context);
//  }
//  public R visitDateTime(DateTimeType type, C context) {
//    return visitScalarType(type, context);
//  }
//  public R visitNull(NullType type, C context) {
//    return visitScalarType(type, context);
//  }
//  public R visitUuid(UuidType type, C context) {
//    return visitScalarType(type, context);
//  }
//
//  public R visitFloat(FloatType type, C context) {
//    return visitNumber(type, context);
//  }
//  public R visitInteger(IntegerType type, C context) {
//    return visitNumber(type, context);
//  }


  public R visitRelationDefinition(RelationDefinition relation, C context) {
    return null;
  }

  public R visitImportTableDefinition(ImportRelationDefinition relation, C context) {
    return visitRelationDefinition(relation, context);
  }

  public R visitExtendedRelation(ExtendedFieldRelationDefinition relation, C context) {
    return visitRelationDefinition(relation, context);
  }

  public R visitDistinctRelation(DistinctRelationDefinition relation, C context) {
    return visitRelationDefinition(relation, context);
  }

  public R visitQueryRelationDefinition(QueryRelationDefinition relation, C context) {
    return visitRelationDefinition(relation, context);
  }

  public R visitExtendedChild(ExtendedChildRelationDefinition relation,
      C context) {
    return visitRelationDefinition(relation, context);
  }

  public R visitExtendedChildQueryRelation(
      ExtendedChildQueryRelationDefinition relation, C context) {
    return visitRelationDefinition(relation, context);
  }

  public <F extends Field> R visitRelation(RelationType relationType, C context) {
    return null;
  }
}

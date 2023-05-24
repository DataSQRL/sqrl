package com.datasqrl.packager.preprocess.graphql;

import com.datasqrl.graphql.visitor.GraphqlDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlFieldDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlInputValueDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.graphql.visitor.GraphqlTypeVisitor;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.constraint.NotNull;
import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.schema.type.basic.BooleanType;
import com.datasqrl.schema.type.basic.FloatType;
import com.datasqrl.schema.type.basic.IntegerType;
import com.datasqrl.schema.type.basic.StringType;
import com.google.common.base.Preconditions;
import graphql.language.EnumValueDefinition;
import graphql.language.FieldDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
public class InputFieldToFlexibleSchemaRelation implements
    GraphqlDefinitionVisitor<List<TableDefinition>, Object>,
    GraphqlFieldDefinitionVisitor<TableDefinition, Object> {

  private final TypeDefinitionRegistry typeDefinitionRegistry;

  @Override
  public List<TableDefinition> visitObjectTypeDefinition(ObjectTypeDefinition node,
      Object context) {

    //node is: mutation
    List<TableDefinition> schemas = node.getFieldDefinitions().stream()
        .map(f->GraphqlSchemaVisitor.accept(this, f, context))
        .collect(Collectors.toList());

    return schemas;
  }

  @Override
  public TableDefinition visitFieldDefinition(FieldDefinition node, Object context) {
//    validateReturnType(fieldType); todo

    Preconditions.checkState(node.getInputValueDefinitions().size() == 1, "Too many arguments for mutation '%s'. Must have exactly one.", node.getName());
    InputValueDefinition def = node.getInputValueDefinitions().get(0);
    Preconditions.checkState(def.getType() instanceof NonNullType, "Mutation '%s' input argument must be non-array and non-null", node.getName());
    Preconditions.checkState(((NonNullType) def.getType()).getType() instanceof TypeName, "Mutation '%s' input argument must be non-array and non-null", node.getName());

    //Todo get description directive
    TypeDefinition typeDef =
        GraphqlSchemaVisitor.accept(new TypeResolver(), ((NonNullType) def.getType()).getType(), typeDefinitionRegistry)
            .orElseThrow(()->new RuntimeException("Could not find type:" + def.getName()));

    com.datasqrl.schema.input.external.FieldDefinition field =
        GraphqlSchemaVisitor.accept(new InputObjectToFlexibleRelation(),
        typeDef, new FieldContext(0, false, node.getName()));

    return new TableDefinition(node.getName(), null, null, "1",
        false,
        field.getColumns(),
       null
    );
  }

  private class InputObjectToFlexibleRelation implements
      GraphqlDefinitionVisitor<com.datasqrl.schema.input.external.FieldDefinition, FieldContext>,
      GraphqlInputValueDefinitionVisitor<com.datasqrl.schema.input.external.FieldDefinition, FieldContext>,
      GraphqlTypeVisitor<com.datasqrl.schema.input.external.FieldDefinition, FieldContext>
  {

    @Override
    public com.datasqrl.schema.input.external.FieldDefinition visitInputObjectTypeDefinition(InputObjectTypeDefinition node,
        FieldContext context) {
      List<com.datasqrl.schema.input.external.FieldDefinition> fields = node.getInputValueDefinitions().stream()
          .map(f->GraphqlSchemaVisitor.accept(this, f, context))
          .collect(Collectors.toList());

      return new com.datasqrl.schema.input.external.FieldDefinition(
          context.name, null, null,
          null, fields, null, null);
    }

    @Override
    public com.datasqrl.schema.input.external.FieldDefinition visitInputValueDefinition(InputValueDefinition node, FieldContext context) {
      return GraphqlSchemaVisitor.accept(this, node.getType(),
          new FieldContext(0, false, node.getName()));
    }

    @Override
    public com.datasqrl.schema.input.external.FieldDefinition visitListType(ListType node, FieldContext context) {

      //nested depth only if the type is a scalar
//      context.nestedDepth++;
      return GraphqlSchemaVisitor.accept(this, node.getType(), context);
    }

    @Override
    public com.datasqrl.schema.input.external.FieldDefinition visitNonNullType(NonNullType node, FieldContext context) {
      context.setNotNull(true);
      return GraphqlSchemaVisitor.accept(this, node.getType(), context);
    }

    @Override
    public com.datasqrl.schema.input.external.FieldDefinition visitTypeName(TypeName node, FieldContext context) {
      TypeDefinition typeDef = typeDefinitionRegistry.getType(node.getName())
          .orElseThrow(()-> new RuntimeException("Could not find node:" + node.getName()));

      return GraphqlSchemaVisitor.accept(this, typeDef, context);
    }

    @Override
    public com.datasqrl.schema.input.external.FieldDefinition visitEnumValueDefinition(EnumValueDefinition node, FieldContext context) {
      return toField(new StringType().getName(), context);
    }

    private List<String> getContraintList(FieldContext context) {
      return context.isNotNull ? List.of(NotNull.INSTANCE.getName().getDisplay()) : null;
    }

    @Override
    public com.datasqrl.schema.input.external.FieldDefinition visitScalarTypeDefinition(ScalarTypeDefinition node, FieldContext context) {
      String type;
      switch (node.getName()) {
        case "Int":
          type = new IntegerType().getName();
          break;
        case "Float":
          type = new FloatType().getName();
          break;
        case "Boolean":
          type = new BooleanType().getName();
          break;
        case "String":
        case "Id":
        default:
          type = new StringType().getName();
          break;
      }

      return toField(type, context);
    }

    private com.datasqrl.schema.input.external.FieldDefinition toField(String type, FieldContext context) {
      return new com.datasqrl.schema.input.external.FieldDefinition(context.name,
          null, null,
          type, null, getContraintList(context), null);
//          Name.system(context.name), SchemaElementDescription.NONE, null,
//          List.of(new FieldType(Name.system(context.name), type,
//              context.nestedDepth, getContraintList(context))));
    }
  }

  @Setter
  @AllArgsConstructor
  private class FieldContext {
    int nestedDepth;
    boolean isNotNull;
    String name;
  }

  private class TypeResolver implements
    GraphqlTypeVisitor<Optional<TypeDefinition>, Object> {

    @Override
    public Optional<TypeDefinition> visitListType(ListType node, Object context) {
      return GraphqlSchemaVisitor.accept(this, node.getType(), context);
    }

    @Override
    public Optional<TypeDefinition> visitNonNullType(NonNullType node, Object context) {
      return GraphqlSchemaVisitor.accept(this, node.getType(), context);
    }

    @Override
    public Optional<TypeDefinition> visitTypeName(TypeName node, Object context) {
      return typeDefinitionRegistry.getType(node.getName());
    }
  }
}

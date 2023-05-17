package com.datasqrl.packager.preprocess.graphql;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.graphql.visitor.GraphqlDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlFieldDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlInputValueDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.graphql.visitor.GraphqlTypeVisitor;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.constraint.NotNull;
import com.datasqrl.schema.input.FlexibleFieldSchema.Field;
import com.datasqrl.schema.input.FlexibleFieldSchema.FieldType;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.RelationType;
import com.datasqrl.schema.type.basic.AbstractBasicType;
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
import graphql.language.Type;
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
    GraphqlDefinitionVisitor<List<FlexibleTableSchema>, Object>,
    GraphqlFieldDefinitionVisitor<FlexibleTableSchema, Object> {

  private final TypeDefinitionRegistry typeDefinitionRegistry;

  @Override
  public List<FlexibleTableSchema> visitObjectTypeDefinition(ObjectTypeDefinition node,
      Object context) {

    //node is: mutation
    List<FlexibleTableSchema> schemas = node.getFieldDefinitions().stream()
        .map(f->GraphqlSchemaVisitor.accept(this, f, context))
        .collect(Collectors.toList());

    return schemas;
  }

  @Override
  public FlexibleTableSchema visitFieldDefinition(FieldDefinition node, Object context) {
//    validateReturnType(fieldType); todo

    Preconditions.checkState(node.getInputValueDefinitions().size() == 1, "Too many arguments for mutation '%s'. Must have exactly one.", node.getName());
    InputValueDefinition def = node.getInputValueDefinitions().get(0);
    Preconditions.checkState(def.getType() instanceof NonNullType, "Mutation '%s' input argument must be non-array and non-null", node.getName());
    Preconditions.checkState(((NonNullType) def.getType()).getType() instanceof TypeName, "Mutation '%s' input argument must be non-array and non-null", node.getName());

    //Todo get description directive
    TypeDefinition typeDef =
        GraphqlSchemaVisitor.accept(new TypeResolver(), ((NonNullType) def.getType()).getType(), typeDefinitionRegistry)
            .orElseThrow(()->new RuntimeException("Could not find type:" + def.getName()));

    Field field = GraphqlSchemaVisitor.accept(new InputObjectToFlexibleRelation(),
        typeDef, new FieldContext(0, false, node.getName()));
    Preconditions.checkState(field.getTypes().get(0).getType() instanceof RelationType, "Only input types on mutations are supported.");
    RelationType relationType = (RelationType)field.getTypes().get(0).getType();

    return new FlexibleTableSchema(
        Name.system(node.getName()),
        null,
        null,
        false,
        relationType,
        List.of()
    );
  }

  private class InputObjectToFlexibleRelation implements
      GraphqlDefinitionVisitor<Field, FieldContext>,
      GraphqlInputValueDefinitionVisitor<Field, FieldContext>,
      GraphqlTypeVisitor<Field, FieldContext>
  {

    @Override
    public Field visitInputObjectTypeDefinition(InputObjectTypeDefinition node,
        FieldContext context) {
      List<Field> fields = node.getInputValueDefinitions().stream()
          .map(f->GraphqlSchemaVisitor.accept(this, f, context))
          .collect(Collectors.toList());

      return toField(new RelationType<>(fields), context);
    }

    @Override
    public Field visitInputValueDefinition(InputValueDefinition node, FieldContext context) {
      return GraphqlSchemaVisitor.accept(this, node.getType(),
          new FieldContext(0, false, node.getName()));
    }

    @Override
    public Field visitListType(ListType node, FieldContext context) {
      context.nestedDepth++;
      return GraphqlSchemaVisitor.accept(this, node.getType(), context);
    }

    @Override
    public Field visitNonNullType(NonNullType node, FieldContext context) {
      context.setNotNull(true);
      return GraphqlSchemaVisitor.accept(this, node.getType(), context);
    }

    @Override
    public Field visitTypeName(TypeName node, FieldContext context) {
      TypeDefinition typeDef = typeDefinitionRegistry.getType(node.getName())
          .orElseThrow(()-> new RuntimeException("Could not find node:" + node.getName()));

      return GraphqlSchemaVisitor.accept(this, typeDef, context);
    }

    @Override
    public Field visitEnumValueDefinition(EnumValueDefinition node, FieldContext context) {
      return toField(new StringType(), context);
    }

    private List<Constraint> getContraintList(FieldContext context) {
      return context.isNotNull ? List.of(NotNull.INSTANCE) : List.of();
    }

    @Override
    public Field visitScalarTypeDefinition(ScalarTypeDefinition node, FieldContext context) {
      AbstractBasicType type;
      switch (node.getName()) {
        case "Int":
          type = new IntegerType();
          break;
        case "Float":
          type = new FloatType();
          break;
        case "Boolean":
          type = new BooleanType();
          break;
        case "String":
        case "Id":
        default:
          type = new StringType();
          break;
      }

      return toField(type, context);
    }

    private Field toField(com.datasqrl.schema.type.Type type, FieldContext context) {
      return new Field(Name.system(context.name), null, null,
          List.of(new FieldType(Name.system(context.name), type,
              context.nestedDepth, getContraintList(context))));
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

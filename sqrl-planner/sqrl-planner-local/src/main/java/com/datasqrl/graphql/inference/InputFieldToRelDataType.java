package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getMutationTypeName;

import com.datasqrl.calcite.type.NamedRelDataType;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.visitor.GraphqlDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlFieldDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlInputValueDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.graphql.visitor.GraphqlTypeVisitor;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.UniversalTable.Configuration;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

@AllArgsConstructor
public class InputFieldToRelDataType implements
    GraphqlDefinitionVisitor<List<NamedRelDataType>, TypeDefinitionRegistry>,
    GraphqlFieldDefinitionVisitor<NamedRelDataType, TypeDefinitionRegistry> {

  private final TypeDefinitionRegistry typeDefinitionRegistry;
  private final RelDataTypeFactory typeFactory;
  private final NameCanonicalizer canonicalizer;

  @Override
  public List<NamedRelDataType> visitObjectTypeDefinition(ObjectTypeDefinition node,
      TypeDefinitionRegistry context) {
    Preconditions.checkArgument(node.getName().equals(getMutationTypeName(context)),
        "Mutation is required");
    List<NamedRelDataType> schemas = node.getFieldDefinitions().stream()
        .map(f->GraphqlSchemaVisitor.accept(this, f, context))
        .collect(Collectors.toList());

    return schemas;
  }

  @Override
  public NamedRelDataType visitFieldDefinition(FieldDefinition node, TypeDefinitionRegistry context) {
//    validateReturnType(fieldType); todo

    Preconditions.checkState(node.getInputValueDefinitions().size() == 1, "Too many arguments for mutation '%s'. Must have exactly one.", node.getName());
    InputValueDefinition def = node.getInputValueDefinitions().get(0);
    Preconditions.checkState(def.getType() instanceof NonNullType, "Mutation '%s' input argument must be non-array and non-null", node.getName());
    Preconditions.checkState(((NonNullType) def.getType()).getType() instanceof TypeName, "Mutation '%s' input argument must be non-array and non-null", node.getName());

    //Todo get description directive
    TypeDefinition typeDef =
        GraphqlSchemaVisitor.accept(new TypeResolver(), ((NonNullType) def.getType()).getType(), typeDefinitionRegistry)
            .orElseThrow(()->new RuntimeException("Could not find type:" + def.getName()));

    RelDataType relDataType = GraphqlSchemaVisitor.accept(new InputObjectToRelDataType(),
        typeDef, new FieldContext());

    return new NamedRelDataType(Name.system(node.getName()), relDataType);
  }

  private class InputObjectToRelDataType implements
      GraphqlDefinitionVisitor<RelDataType, FieldContext>,
      GraphqlInputValueDefinitionVisitor<RelDataType, FieldContext>,
      GraphqlTypeVisitor<RelDataType, FieldContext>
  {

    @Override
    public RelDataType visitInputObjectTypeDefinition(InputObjectTypeDefinition node,
        FieldContext context) {
      RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
      node.getInputValueDefinitions().forEach(field ->
          typeBuilder.add(field.getName(), GraphqlSchemaVisitor.accept(this, field,
              new FieldContext())));
      return typeBuilder.build();
    }

    @Override
    public RelDataType visitInputValueDefinition(InputValueDefinition node, FieldContext context) {
      return GraphqlSchemaVisitor.accept(this, node.getType(), context);
    }

    @Override
    public RelDataType visitListType(ListType node, FieldContext context) {
      RelDataType type = typeFactory.createTypeWithNullability(
          TypeFactory.wrapInArray(typeFactory, GraphqlSchemaVisitor.accept(this, node.getType(), context)),
          true);
      return type;
    }

    @Override
    public RelDataType visitNonNullType(NonNullType node, FieldContext context) {
      return TypeFactory.withNullable(typeFactory,
          GraphqlSchemaVisitor.accept(this, node.getType(), context),
          false);
    }

    @Override
    public RelDataType visitTypeName(TypeName node, FieldContext context) {
      TypeDefinition typeDef = typeDefinitionRegistry.getType(node.getName())
          .orElseThrow(()-> new RuntimeException("Could not find node:" + node.getName()));

      return GraphqlSchemaVisitor.accept(this, typeDef, context);
    }

    @Override
    public RelDataType visitEnumValueDefinition(EnumValueDefinition node, FieldContext context) {
      return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, Short.MAX_VALUE),true);
    }

    @Override
    public RelDataType visitScalarTypeDefinition(ScalarTypeDefinition node, FieldContext context) {
      RelDataType type;
      switch (node.getName()) {
        case "Int":
          type = typeFactory.createSqlType(SqlTypeName.BIGINT);
          break;
        case "Float":
          type = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 5);
          break;
        case "Boolean":
          type = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
          break;
        case "DateTime":
          type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 9);
          break;
        case "String":
        case "ID":
          type = typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
          break;
        default:
          throw new RuntimeException("Unknown Type");
      }
      return typeFactory.createTypeWithNullability(type, true);
    }
  }

  @Setter
  @AllArgsConstructor
  private class FieldContext {
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

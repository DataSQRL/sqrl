package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getMutationTypeName;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.checkState;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.createThrowable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.visitor.GraphqlDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlFieldDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlInputValueDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.graphql.visitor.GraphqlTypeVisitor;
import com.datasqrl.util.CalciteUtil;

import graphql.language.EnumTypeDefinition;
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
import lombok.AllArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
public class InputFieldToRelDataType implements
    GraphqlDefinitionVisitor<List<RelDataTypeField>, TypeDefinitionRegistry>,
    GraphqlFieldDefinitionVisitor<RelDataTypeField, TypeDefinitionRegistry> {

  private final TypeDefinitionRegistry typeDefinitionRegistry;
  private final RelDataTypeFactory typeFactory;
  private final NameCanonicalizer canonicalizer;

  @Override
  public List<RelDataTypeField> visitObjectTypeDefinition(ObjectTypeDefinition node,
      TypeDefinitionRegistry context) {
    checkState(node.getName().equals(getMutationTypeName(context)), node.getSourceLocation(),
        "Mutation is required");
    List<RelDataTypeField> schemas = node.getFieldDefinitions().stream()
        .map(f->GraphqlSchemaVisitor.accept(this, f, context))
        .collect(Collectors.toList());

    return schemas;
  }

  @Override
  public RelDataTypeField visitFieldDefinition(FieldDefinition node, TypeDefinitionRegistry context) {
//    validateReturnType(fieldType); todo

    checkState(node.getInputValueDefinitions().size() == 1, node.getSourceLocation(),"Too many arguments for mutation '%s'. Must have exactly one.", node.getName());
    var def = node.getInputValueDefinitions().get(0);
    checkState(def.getType() instanceof NonNullType, node.getSourceLocation(), "Mutation '%s' input argument must be non-array and non-null", node.getName());
    checkState(((NonNullType) def.getType()).getType() instanceof TypeName,  node.getSourceLocation(),"Mutation '%s' input argument must be non-array and non-null", node.getName());

    var typeDef =
        GraphqlSchemaVisitor.accept(new TypeResolver(), ((NonNullType) def.getType()).getType(), typeDefinitionRegistry)
            .orElseThrow(()->createThrowable( node.getSourceLocation(),"Could not find type:" + def.getName()));

    var relDataType = GraphqlSchemaVisitor.accept(new InputObjectToRelDataType(),
        typeDef, new FieldContext());

    return new RelDataTypeFieldImpl(node.getName(), -1, relDataType);
  }

  private class InputObjectToRelDataType implements
      GraphqlDefinitionVisitor<RelDataType, FieldContext>,
      GraphqlInputValueDefinitionVisitor<RelDataType, FieldContext>,
      GraphqlTypeVisitor<RelDataType, FieldContext>
  {

    @Override
    public RelDataType visitInputObjectTypeDefinition(InputObjectTypeDefinition node,
        FieldContext context) {
      var typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
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
      var type = typeFactory.createTypeWithNullability(
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
      var typeDef = typeDefinitionRegistry.getType(node.getName())
          .orElseThrow(()-> new RuntimeException("Could not find node:" + node.getName()));

      return GraphqlSchemaVisitor.accept(this, typeDef, context);
    }

    @Override
    public RelDataType visitEnumTypeDefinition(EnumTypeDefinition node, FieldContext context) {
      return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, Short.MAX_VALUE),true);
    }

    @Override
    public RelDataType visitEnumValueDefinition(EnumValueDefinition node, FieldContext context) {
      return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, Short.MAX_VALUE),true);
    }

    @Override
    public RelDataType visitScalarTypeDefinition(ScalarTypeDefinition node, FieldContext context) {
      var type = switch (node.getName()) {
	case "Int" -> typeFactory.createSqlType(SqlTypeName.BIGINT);
	case "Float" -> typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 5);
	case "Boolean" -> typeFactory.createSqlType(SqlTypeName.BOOLEAN);
	case "DateTime" -> typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
	case "String", "ID" -> typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
	default -> throw new RuntimeException("Unknown Type");
	};
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

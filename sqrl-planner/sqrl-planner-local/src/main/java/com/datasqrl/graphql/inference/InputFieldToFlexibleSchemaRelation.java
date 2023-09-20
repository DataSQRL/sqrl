package com.datasqrl.graphql.inference;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.visitor.GraphqlDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlFieldDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlInputValueDefinitionVisitor;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.graphql.visitor.GraphqlTypeVisitor;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.UniversalTable;
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
public class InputFieldToFlexibleSchemaRelation implements
    GraphqlDefinitionVisitor<List<UniversalTable>, Object>,
    GraphqlFieldDefinitionVisitor<UniversalTable, Object> {

  private final TypeDefinitionRegistry typeDefinitionRegistry;
  private final RelDataTypeFactory typeFactory;
  private final NameCanonicalizer canonicalizer;

  @Override
  public List<UniversalTable> visitObjectTypeDefinition(ObjectTypeDefinition node,
      Object context) {
    Preconditions.checkArgument(node.getName().equals("Mutation"),"mutation");
    List<UniversalTable> schemas = node.getFieldDefinitions().stream()
        .map(f->GraphqlSchemaVisitor.accept(this, f, context))
        .collect(Collectors.toList());

    return schemas;
  }

  @Override
  public UniversalTable visitFieldDefinition(FieldDefinition node, Object context) {
//    validateReturnType(fieldType); todo

    Preconditions.checkState(node.getInputValueDefinitions().size() == 1, "Too many arguments for mutation '%s'. Must have exactly one.", node.getName());
    InputValueDefinition def = node.getInputValueDefinitions().get(0);
    Preconditions.checkState(def.getType() instanceof NonNullType, "Mutation '%s' input argument must be non-array and non-null", node.getName());
    Preconditions.checkState(((NonNullType) def.getType()).getType() instanceof TypeName, "Mutation '%s' input argument must be non-array and non-null", node.getName());

    //Todo get description directive
    TypeDefinition typeDef =
        GraphqlSchemaVisitor.accept(new TypeResolver(), ((NonNullType) def.getType()).getType(), typeDefinitionRegistry)
            .orElseThrow(()->new RuntimeException("Could not find type:" + def.getName()));

    return
        GraphqlSchemaVisitor.accept(new InputObjectToFlexibleRelation(),
        typeDef, new FieldContext(canonicalizer.name(node.getName()),null, 0, true));
  }

  private class InputObjectToFlexibleRelation implements
      GraphqlDefinitionVisitor<UniversalTable, FieldContext>,
      GraphqlInputValueDefinitionVisitor<UniversalTable, FieldContext>,
      GraphqlTypeVisitor<UniversalTable, FieldContext>
  {

    @Override
    public UniversalTable visitInputObjectTypeDefinition(InputObjectTypeDefinition node,
        FieldContext context) {

      UniversalTable table;
      Name tableName = context.name;
      if (context.parent!=null) {
        //making it a singleton to keep primary keys at 0
        table = new UniversalTable(tableName, context.parent.getPath().concat(tableName), context.parent, true, false);

        Preconditions.checkArgument(context.listDepth<=1, "Do not support multi-dimensional arrays of types");
        Multiplicity multiplicity = Multiplicity.ZERO_ONE;
        if (context.listDepth>0) multiplicity = Multiplicity.MANY;
        else if (context.isNotNull) multiplicity = Multiplicity.ONE;
        context.parent.addChild(context.name, table, multiplicity);
      } else {
        table = new UniversalTable(tableName, NamePath.of(tableName), 0, false);
      }

      FieldContext nestedContext = new FieldContext(null, table, 0, false);
      node.getInputValueDefinitions().forEach(field ->
          GraphqlSchemaVisitor.accept(this, field, nestedContext));
      return table;
    }

    @Override
    public UniversalTable visitInputValueDefinition(InputValueDefinition node, FieldContext context) {
      return GraphqlSchemaVisitor.accept(this, node.getType(),
          new FieldContext(canonicalizer.name(node.getName()), context.parent, 0, false));
    }

    @Override
    public UniversalTable visitListType(ListType node, FieldContext context) {
      context.setListDepth(context.listDepth+1);
      context.setNotNull(false);
      return GraphqlSchemaVisitor.accept(this, node.getType(), context);
    }

    @Override
    public UniversalTable visitNonNullType(NonNullType node, FieldContext context) {
      context.setNotNull(true);
      return GraphqlSchemaVisitor.accept(this, node.getType(), context);
    }

    @Override
    public UniversalTable visitTypeName(TypeName node, FieldContext context) {
      TypeDefinition typeDef = typeDefinitionRegistry.getType(node.getName())
          .orElseThrow(()-> new RuntimeException("Could not find node:" + node.getName()));

      return GraphqlSchemaVisitor.accept(this, typeDef, context);
    }

    @Override
    public UniversalTable visitEnumValueDefinition(EnumValueDefinition node, FieldContext context) {
      return addColumn(typeFactory.createSqlType(SqlTypeName.VARCHAR, Short.MAX_VALUE), context);
    }

    @Override
    public UniversalTable visitScalarTypeDefinition(ScalarTypeDefinition node, FieldContext context) {
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
      return addColumn(type, context);
    }

    private UniversalTable addColumn(RelDataType type, FieldContext context) {
      context.parent.addColumn(context.name, addContextToType(type,context));
      return context.parent;
    }

    private RelDataType addContextToType(RelDataType type, FieldContext context) {
      type = TypeFactory.withNullable(typeFactory, type, !context.isNotNull);
      //wrap in array of nested depth;
      for (int i = 0; i < context.listDepth; i++) {
        type = typeFactory.createArrayType(type, -1L);
      }
      return type;
    }
  }

  @Setter
  @AllArgsConstructor
  private class FieldContext {
    Name name;
    UniversalTable parent;
    int listDepth;
    boolean isNotNull;
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

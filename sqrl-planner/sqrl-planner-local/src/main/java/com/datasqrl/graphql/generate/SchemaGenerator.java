/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getInputType;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getOutputType;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.wrap;
import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;
import static graphql.schema.GraphQLNonNull.nonNull;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.plan.table.LogicalNestedTable;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.google.common.base.Preconditions;
import graphql.Scalars;
import graphql.language.IntValue;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.commons.collections.ListUtils;

/**
 * Creates a default graphql schema based on the SQRL schema
 */
public class SchemaGenerator {

  List<GraphQLFieldDefinition> queryFields = new ArrayList<>();
  List<GraphQLObjectType> objectTypes = new ArrayList<>();
  private boolean addArguments;
  Map<NamePath, List<SqrlTableMacro>> tables;
  private SqrlSchema schema;

  public static boolean isValidGraphQLName(String name) {
    return Pattern.matches("[A-Za-z][_0-9A-Za-z]*", name);
  }

  public GraphQLSchema generate(SqrlSchema schema, boolean addArguments) {
    this.addArguments = addArguments;
    this.schema = schema;

    Map<NamePath, List<SqrlTableMacro>> relationships = schema.getTableFunctions().stream()
        .collect(Collectors.groupingBy(e -> e.getFullPath().popLast(),
            LinkedHashMap::new, Collectors.toList()));

    tables = schema.getTableFunctions().stream()
        .collect(Collectors.groupingBy(e -> e.getAbsolutePath(),
            LinkedHashMap::new, Collectors.toList()));

    for (Map.Entry<NamePath, List<SqrlTableMacro>> path : tables.entrySet()) {
      Optional<GraphQLObjectType> graphQLObjectType = generate2(path.getValue(),
          relationships.getOrDefault(path.getKey(), List.of()));
      graphQLObjectType.map(o->objectTypes.add(o));
    }

    GraphQLObjectType queryType = createQueryType(relationships.get(NamePath.ROOT));

//    postProcess();

//    if (queryFields.isEmpty()) {
//      throw new RuntimeException("No tables found to build schema");
//    }

    return GraphQLSchema.newSchema()
        .query(queryType)
        .additionalTypes(new LinkedHashSet<>(objectTypes))
        .additionalType(CustomScalars.DATETIME)
        .build();
  }

  private GraphQLObjectType createQueryType(List<SqrlTableMacro> relationships) {

    List<GraphQLFieldDefinition> fields = new ArrayList<>();

    for (SqrlTableMacro rel : relationships) {
      if (rel.getFullPath().size() > 1) continue; //skip nested hack
      GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
          .name(rel.getAbsolutePath().getDisplay())
          .type(wrap(createTypeName(rel), rel.getMultiplicity()))
          .arguments(createArguments(rel))
          .build();
      fields.add(field);
    }

    GraphQLObjectType objectType = GraphQLObjectType.newObject()
        .name("Query")
        .fields(fields)
        .build();

    return objectType;
  }

  private Optional<GraphQLObjectType> generate2(List<SqrlTableMacro> tableMacros, List<SqrlTableMacro> relationships) {
    //The multiple table macros should all point to the same relnode type

    Map<Name, List<SqrlTableMacro>> relByName = relationships.stream()
        .collect(Collectors.groupingBy(g -> g.getFullPath().getLast(),
            LinkedHashMap::new, Collectors.toList()));

    SqrlTableMacro first = tableMacros.get(0);
    RelDataType rowType = first.getRowType();
    for (int i = 1; i < tableMacros.size(); i++) {
      RelDataType type = tableMacros.get(i).getRowType();
      boolean isEqual = rowType.equalsSansFieldNames(type);
      Preconditions.checkState(isEqual, "Table macro type mismatch in sqrl schema: {} expected {}",
          type, rowType);
      rowType = type;
    }
    String name = generateObjectName(first.getAbsolutePath());

    List<GraphQLFieldDefinition> fields = new ArrayList<>();
    //relByName
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (!relByName.containsKey(Name.system(field.getName()))) {
        createRelationshipField(field).map(fields::add);
      }
    }

    for (Map.Entry<Name, List<SqrlTableMacro>> rel : relByName.entrySet()) {
      createRelationshipField(rel.getValue()).map(fields::add);
    }

    if (fields.isEmpty()) {
      return Optional.empty();
    }

    GraphQLObjectType objectType = GraphQLObjectType.newObject()
        .name(name)
        .fields(fields)
        .build();

    return Optional.of(objectType);
  }

  //Todo: add uniqueness
  private String generateObjectName(NamePath path) {
    if (path.isEmpty()) {
      return "Query";
    }
    return path.getLast().getDisplay();
  }

  private Optional<GraphQLFieldDefinition> createRelationshipField(List<SqrlTableMacro> value) {
    SqrlTableMacro sqrlTableMacro = value.get(0);
    String name = sqrlTableMacro.getFullPath().getLast().getDisplay();
    if (!isValidGraphQLName(name)) {
      return Optional.empty();
    }

    GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
        .name(name)
        .type(wrap(createTypeName(sqrlTableMacro), sqrlTableMacro.getMultiplicity()))
        .arguments(createArguments(sqrlTableMacro))
        .build();

    return Optional.of(field);
  }

  private List<GraphQLArgument> createArguments(SqrlTableMacro field) {
    if (!allowedArguments(field)) {
      return List.of();
    }

    List<FunctionParameter> parameters = field.getParameters().stream()
        .filter(f->!((SqrlFunctionParameter)f).isInternal())
        .collect(Collectors.toList());
    if (addArguments && parameters.isEmpty() && field.getJoinType() == JoinType.JOIN) {
      List<GraphQLArgument> limitOffset = generateLimitOffset();
      return limitOffset;
    } else if (addArguments && parameters.isEmpty()) {
      NamePath toTable = schema.getPathToAbsolutePathMap()
          .get(field.getFullPath());
      List<SqrlTableMacro> sqrlTableMacros = tables.get(toTable);
      List<GraphQLArgument> premuted = generatePermuted(sqrlTableMacros.get(0));
      List<GraphQLArgument> limitOffset = generateLimitOffset();

      return ListUtils.union(premuted, limitOffset);
    } else {
      return parameters.stream()
          .filter(p->!((SqrlFunctionParameter)p).isInternal())
          .filter(p->getInputType(p.getType(null)).isPresent())
          .map(parameter -> GraphQLArgument.newArgument()
              .name(((SqrlFunctionParameter)parameter).getVariableName())
              .type(nonNull(getInputType(parameter.getType(null)).get()))
              .build()).collect(Collectors.toList());
    }
  }

  private boolean allowedArguments(SqrlTableMacro field) {
    //No arguments for to-one rels or parent fields
    return field.getMultiplicity().equals(Multiplicity.MANY) &&
        !field.getJoinType().equals(JoinType.PARENT);
  }

  private List<GraphQLArgument> generateLimitOffset() {

    //add limit / offset
    GraphQLArgument limit = GraphQLArgument.newArgument()
        .name(LIMIT)
        .type(Scalars.GraphQLInt)
        .defaultValueLiteral(IntValue.of(10))
        .build();

    GraphQLArgument offset = GraphQLArgument.newArgument()
        .name(OFFSET)
        .type(Scalars.GraphQLInt)
        .defaultValueLiteral(IntValue.of(0))
        .build();
    return List.of(limit, offset);
  }

  private List<GraphQLArgument> generatePermuted(SqrlTableMacro macro) {
    String tableName = schema.getPathToSysTableMap().get(macro.getAbsolutePath());
    Table table = schema.getTable(tableName, false).getTable();
    RelDataType rowType = macro.getRowType();

    List<RelDataTypeField> primaryKeys = new ArrayList<>();
    for (int i = 0; i < rowType.getFieldList().size(); i++) {
      boolean localPrimaryKey = isLocalPrimaryKey(table, i);
      if (localPrimaryKey) {
        primaryKeys.add(rowType.getFieldList().get(i));
      }
    }

    return
        primaryKeys
        .stream()
        .filter(f -> getInputType(f.getType()).isPresent())
        .filter(f -> isValidGraphQLName(f.getName()))
        .map(f -> GraphQLArgument.newArgument()
            .name(f.getName())
            .type(getInputType(f.getType()).get())
            .build())
        .collect(Collectors.toList());
  }
  private boolean isLocalPrimaryKey(Table table, int i) {
    if (table instanceof LogicalNestedTable) {
      LogicalNestedTable nestedTable = (LogicalNestedTable) table;
      if (i < nestedTable.getNumPrimaryKeys() - nestedTable.getNumLocalPks()) {
        return false;
      } else if (i < nestedTable.getNumPrimaryKeys()) {
        return true;
      }
    } else if (table instanceof ScriptRelationalTable) {
      ScriptRelationalTable relTable = (ScriptRelationalTable) table;
      if (i < relTable.getNumPrimaryKeys()) {
        return true;
      }
    }

    return false;
  }

  Map<NamePath, String> uniqueNameMap = new HashMap<>();

  private GraphQLOutputType createTypeName(SqrlTableMacro sqrlTableMacro) {
    return new GraphQLTypeReference(sqrlTableMacro.getAbsolutePath().getLast().getDisplay());
  }


  private Optional<GraphQLFieldDefinition> createRelationshipField(RelDataTypeField field) {
    return getOutputType(field.getType())
        .filter(f->isValidGraphQLName(field.getName()))
        .map(t -> GraphQLFieldDefinition.newFieldDefinition()
            .name(field.getName())
            .type(wrap(t, field.getType())).build());
  }

  private void generate(SqrlTableMacro t) {
    // 1 group
    // 2 collesce
    // 3

    NamePath path = t.getAbsolutePath();
    //If path is 1, add to Query table

    //If we've seen this path before, look at parameters and adjust nullability

  }

  public void postProcess() {
    // Ensure every field points to a valid type
    boolean found;
    int attempts = 10;
    do {
      found = false;
      Iterator<GraphQLObjectType> iterator = objectTypes.iterator();
      List<GraphQLObjectType> replacedType = new ArrayList<>();
      while (iterator.hasNext()) {
        GraphQLObjectType objectType = iterator.next();
        List<GraphQLFieldDefinition> invalidFields = new ArrayList<>();

        for (GraphQLFieldDefinition field : objectType.getFields()) {
          if (!isValidType(field.getType())) {
            invalidFields.add(field);
          }
        }

        // Refactor to remove invalid fields
        List<GraphQLFieldDefinition> fields = new ArrayList<>(objectType.getFields());
        boolean fieldsRemoved = fields.removeAll(invalidFields);

        // After removing invalid fields, if an object has no fields, it should be removed
        if (fields.isEmpty()) {
          iterator.remove();
          found = true;
        } else if (fieldsRemoved) {
          GraphQLObjectType newObjectType = GraphQLObjectType.newObject(objectType).clearFields()
              .fields(fields).build();
          replacedType.add(newObjectType);
          iterator.remove();
          found = true;
        }
      }

      //Add new types back
      objectTypes.addAll(replacedType);

      found |= queryFields.removeIf(field -> !isValidType(field.getType()));

      // Ensure each object has at least one field
      found |= objectTypes.removeIf(objectType -> objectType.getFields().isEmpty());
    } while (found && --attempts != 0);

    if (found) {
      throw new RuntimeException("Schema too complexity high, could not be reduced");
    }
  }

  boolean isValidType(GraphQLType type) {
    type = unbox(type);
    // You can expand this logic depending on the intricacies of type validation
    if (type instanceof GraphQLTypeReference) {
      GraphQLTypeReference typeReference = (GraphQLTypeReference) type;
      for (GraphQLObjectType objectType : this.objectTypes) {
        if (typeReference.getName().equalsIgnoreCase(objectType.getName())) {
          return true;
        }
      }
    }

    return isBaseGraphQLType(type);
  }

  private GraphQLType unbox(GraphQLType type) {
    if (type instanceof GraphQLNonNull) {
      return unbox(((GraphQLNonNull) type).getWrappedType());
    } else if (type instanceof GraphQLList) {
      return unbox(((GraphQLList) type).getWrappedType());
    }
    return type;
  }

  boolean isBaseGraphQLType(GraphQLType type) {
    return type instanceof GraphQLScalarType;
  }
}
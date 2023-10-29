//package com.datasqrl.graphql.inference;
//
//import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;
//import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getInputType;
//import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getOutputType;
//
//import com.datasqrl.calcite.function.SqrlTableMacro;
//import com.datasqrl.calcite.type.SqrlType;
//import com.datasqrl.canonicalizer.NamePath;
//import com.datasqrl.function.SqrlFunctionParameter;
//import com.datasqrl.schema.RootSqrlTable;
//import graphql.schema.GraphQLArgument;
//import graphql.schema.GraphQLFieldDefinition;
//import graphql.schema.GraphQLObjectType;
//import graphql.schema.GraphQLOutputType;
//import graphql.schema.GraphQLScalarType;
//import graphql.schema.GraphQLSchema;
//import graphql.schema.GraphQLType;
//import graphql.schema.GraphQLTypeReference;
//import graphql.schema.GraphQLUnionType;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.LinkedHashSet;
//import java.util.List;
//import java.util.Set;
//import java.util.regex.Pattern;
//import java.util.stream.Collectors;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.calcite.jdbc.SqrlSchema;
//import org.apache.calcite.rel.type.RelDataTypeField;
//
//@Slf4j
//public class GraphQLSchemaGenerator {
//
//  List<GraphQLFieldDefinition> queryFields = new ArrayList<>();
//  List<GraphQLObjectType> objectTypes = new ArrayList<>();
//  Set<GraphQLType> types;
//
//  public GraphQLSchemaGenerator() {
//    types = new HashSet<>();
//  }
//
//  public GraphQLSchema generateSchema(SqrlSchema schema) {
//    for (SqrlType type : schema.getTypes()) {
//      createType(type);
//    }
//
//    /**
//     * 1. Walk all 'tables'
//     * 2.
//     *
//     */
//
//    List<SqrlTableMacro> tableFunctions = schema.getTableFunctions();
//    for (SqrlTableMacro table : tableFunctions) {
//      createObjectType(table, tableFunctions);
//    }
//
//    for (SqrlTableMacro table : tableFunctions) {
//      if (table.getPath().size() == 1) {
//        addQueryField(table);
//      }
//    }
//
//    postProcess();
//
//    if (queryFields.isEmpty()) {
//      throw new RuntimeException("No tables found to build schema");
//    }
//
//    GraphQLObjectType query = GraphQLObjectType.newObject()
//        .name("Query")
//        .fields(queryFields)
//        .build();
//
//    return GraphQLSchema.newSchema()
//        .query(query)
//        .additionalTypes(types)
//        .additionalTypes(new LinkedHashSet<>(objectTypes))
//        .build();
//  }
//
//  private void addQueryField(SqrlTableMacro table) {
//    String name = table.getPath().get(0).getDisplay();
//    if (!isValidGraphQLName(name) || name.startsWith("_")) {
//      return;
//    }
//
//    GraphQLOutputType type = new GraphQLTypeReference(name);
//
//    List<GraphQLArgument> arguments = permuteArgs(table);
//    if (arguments == null) { //could not permute args
//      return;
//    }
//
//    GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
//        .name(name)
//        .type(type)
//        .arguments(arguments)
//        .build();
//
//    queryFields.add(field);
//  }
//
//  private List<GraphQLArgument> permuteArgs(SqrlTableMacro table) {
//    if (!(table instanceof RootSqrlTable)) {
//      return List.of();
//    }
//    RootSqrlTable sqrlTable = (RootSqrlTable) table;
//    if (sqrlTable.getParameters().isEmpty()) {
//      //permute first few args
//      return sqrlTable.getRowType().getFieldList().stream()
//          .filter(f -> getInputType(f.getType()).isPresent())
//          .filter(f -> isValidGraphQLName(f.getName()))
//          .filter(f -> !f.getName().startsWith(HIDDEN_PREFIX))
//          .map(f -> GraphQLArgument.newArgument()
//              .name(f.getName())
//              .type(getInputType(f.getType()).get())
//              .build())
//          .limit(6)
//          .collect(Collectors.toList());
//    } else {
//      if (sqrlTable.getParameters().stream()
//          .anyMatch(p->((SqrlFunctionParameter)p).isInternal())) {
//        return null;
//      }
//
//      return sqrlTable.getParameters().stream()
//          .map(p->(SqrlFunctionParameter) p)
//          .filter(p->!p.isInternal())
//          .filter(p->getInputType(p.getType(null)).isPresent())
//          .map(parameter -> GraphQLArgument.newArgument()
//              .name(parameter.getVariableName())
//              .type(getInputType(parameter.getType(null)).get())
//              .build()).collect(Collectors.toList());
//    }
//  }
//
//  private void createType(SqrlType type) {
//    GraphQLScalarType scalarType = GraphQLScalarType.newScalar()
//        .name(type.getName())
//        .build();
//    this.types.add(scalarType);
//  }
//
//  void createObjectType(SqrlTableMacro table, List<SqrlTableMacro> tableFunctions) {
//    if (objectTypes.stream().anyMatch(o->o.getName().equalsIgnoreCase(table.getDisplayName()))) {
//      log.warn("Skipping duplicate table:" + table.getDisplayName());
//      return;
//    }
//    GraphQLObjectType.Builder objectBuilder = GraphQLObjectType.newObject();
//    String objectName = toName(table.getPath());
//    objectBuilder.name(objectName);
//
//    for (RelDataTypeField column : table.getRowType().getFieldList()) {
//      if (column.getName().startsWith(HIDDEN_PREFIX)) {
//        continue;
//      }
//
//      if (!isValidGraphQLName(column.getName())) {
//        log.warn("Skipping column in graphql schema, not a valid graphql name: {}:{}", objectName, table.getDisplayName());
//        continue;
//      }
//
//      if (getOutputType(column.getType()).isPresent()) {
//        objectBuilder.field(createRegularField(column));
//      }
//    }
//
//
////
////    Map<String, List<Column>> aggregatedColumns = new HashMap<>();
////    for (Column column : table.getColumns()) {
////      aggregatedColumns
////          .computeIfAbsent(column.getName(), k -> new ArrayList<>())
////          .add(column);
////    }
////
////    for (Map.Entry<String, List<Column>> entry : aggregatedColumns.entrySet()) {
////      List<Column> columns = entry.getValue();
////      if (columns.size() != 1) {
////        log.warn("Skipping column: {}. Reason: Too many parameter matches", columns.get(0).getName());
////        continue;
////      }
////      Column column = columns.get(0);
////      walkTables(column.getIsA());
////
////      List<GraphQLArgument> arguments = column.getParameters().stream()
////          .map(this::createArgument)
////          .collect(Collectors.toList());
////
////      GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
////          .name(entry.getKey())
////          .type(getOutputGraphQLTypeForRelDataType(column.getType()))
////          .arguments(arguments)
////          .build();
////
////      // Now add this union type to your object builder.
////      objectBuilder.field(field);
////    }
//
//    objectTypes.add(objectBuilder.build());
//  }
////
////  private GraphQLArgument createArgument(SqrlFunctionParameter parameter) {
////    GraphQLInputType argType = getInputGraphQLTypeForRelDataType(parameter.getRelDataType());
////    if (parameter.getDefaultValue().isPresent()) {
////      return GraphQLArgument.newArgument()
////          .name(parameter.getName())
////          .type(argType)
////          .defaultValue(((SqlLiteral)parameter.getDefaultValue().get()).getValue()) // Note: Convert SqlNode to an appropriate Java type.
////          .build();
////    } else {
////      return GraphQLArgument.newArgument()
////          .name(parameter.getName())
////          .type(argType)
////          .build();
////    }
////  }
//
////  private GraphQLInputType getInputGraphQLTypeForRelDataType(RelDataType relDataType) {
////    return getInputType(relDataType);
////  }
//
////  private GraphQLOutputType getOutputGraphQLTypeForRelDataType(RelDataType relDataType) {
////    return getOutputType(relDataType);
////  }
//
////  private void walkTables(List<SqrlTableMacro> isA) {
////    for (SqrlTableMacro table : isA) {
////      createObjectType(table, tableFunctions);
////    }
////  }
//
//  private String toName(NamePath path) {
//    return String.join("_", path.toStringList());
//  }
//
//  GraphQLFieldDefinition createRegularField(RelDataTypeField column) {
//    return GraphQLFieldDefinition.newFieldDefinition()
//        .name(column.getName())
//        .type(getOutputType(column.getType()).get())
//        .build();
//  }
//
//  public static boolean isValidGraphQLName(String name) {
//    return Pattern.matches("[_A-Za-z][_0-9A-Za-z]*", name);
//  }
//
//  void postProcess() {
//
//    // Ensure every field points to a valid type
//    Iterator<GraphQLObjectType> iterator = objectTypes.iterator();
//    while (iterator.hasNext()) {
//      GraphQLObjectType objectType = iterator.next();
//      List<GraphQLFieldDefinition> invalidFields = new ArrayList<>();
//
//      for (GraphQLFieldDefinition field : objectType.getFields()) {
//        if (!isValidType(field.getType())) {
//          invalidFields.add(field);
//        }
//      }
//
//      // Refactor to remove invalid fields
//      List<GraphQLFieldDefinition> fields = new ArrayList<>(objectType.getFields());
//      fields.removeAll(invalidFields);
//
//      // After removing invalid fields, if an object has no fields, it should be removed
//      if (fields.isEmpty()) {
//        iterator.remove();
//      }
//    }
//
//    queryFields.removeIf(field -> !isValidType(field.getType()));
//
//    // Ensure each object has at least one field
//    objectTypes.removeIf(objectType -> objectType.getFields().isEmpty());
//  }
//
//  private GraphQLUnionType createUnionType(String name, Set<GraphQLType> possibleTypes) {
//    return GraphQLUnionType.newUnionType()
//        .name(name)
//        .possibleTypes(possibleTypes.toArray(new GraphQLObjectType[0]))
//        .build();
//  }
//
//  boolean isValidType(GraphQLType type) {
//    // You can expand this logic depending on the intricacies of type validation
//    if (type instanceof GraphQLTypeReference) {
//      GraphQLTypeReference typeReference = (GraphQLTypeReference)type;
//      for (GraphQLObjectType objectType : this.objectTypes) {
//        if (typeReference.getName().equalsIgnoreCase(objectType.getName())) {
//          return true;
//        }
//      }
//    }
//
//    return isBaseGraphQLType(type);
//  }
//
//  boolean isBaseGraphQLType(GraphQLType type) {
//    return type instanceof GraphQLScalarType;
//  }
//}

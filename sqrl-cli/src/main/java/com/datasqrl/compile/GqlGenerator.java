/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.compile;

import static com.datasqrl.planner.util.SqrTableFunctionUtil.getTableFunctionFromPath;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.planner.tables.SqrlTableFunction;
import graphql.language.Argument;
import graphql.language.Definition;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.Node;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.OperationDefinition;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.language.VariableDefinition;
import graphql.language.VariableReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

@RequiredArgsConstructor
class GqlGenerator extends GraphqlSchemaVisitor {

  private final List<SqrlTableFunction> tableFunctions;

  @Override
  public List<Node> visitDocument(Document node, Object context) {
    var definitions = node.getDefinitions();

    // Iterate through all queries and process them
    return definitions.stream()
        .filter(definition -> definition instanceof ObjectTypeDefinition)
        .map(definition -> (ObjectTypeDefinition) definition)
        .filter(definition -> definition.getName().equals("Query"))
        .flatMap(q -> processQueryDefinition(q, node).stream())
        .collect(Collectors.toList());
  }

  @Override
  public Object visitObjectTypeDefinition(ObjectTypeDefinition node, Object context) {
    return super.visitObjectTypeDefinition(node, context);
  }

  private List<Node> processQueryDefinition(ObjectTypeDefinition definition, Document document) {
    List<Node> queries = new ArrayList<>();
    for (FieldDefinition def : definition.getFieldDefinitions()) {
      final var tableFunction =
          getTableFunctionFromPath(tableFunctions, NamePath.of(def.getName())).get();
      if (tableFunction.getVisibility().isTest()) {
        var operation =
            processOperation(
                def.getName(),
                (ObjectTypeDefinition) unbox(def.getType(), document).get(),
                def.getInputValueDefinitions(),
                tableFunction.getRowType(),
                document);
        queries.add(operation);
      }
    }
    return queries;
  }

  private OperationDefinition processOperation(
      String name,
      ObjectTypeDefinition type,
      List<InputValueDefinition> inputValueDefinitions,
      RelDataType rowType,
      Document document) {
    var operationBuilder =
        OperationDefinition.newOperationDefinition()
            .name(name)
            .operation(OperationDefinition.Operation.QUERY);

    // Add input value definitions as arguments
    List<VariableDefinition> variableDefinitions =
        inputValueDefinitions.stream()
            .map(input -> new VariableDefinition(input.getName(), input.getType()))
            .collect(Collectors.toList());

    operationBuilder.variableDefinitions(variableDefinitions);
    // Build the selection set recursively for nested fields
    var selectionSet = buildSelectionSet(type, rowType, document);
    // Create the field for the operation
    var fieldBuilder = Field.newField().name(name).selectionSet(selectionSet);

    // Connect arguments to the field
    List<Argument> arguments =
        inputValueDefinitions.stream()
            .map(input -> new Argument(input.getName(), new VariableReference(input.getName())))
            .collect(Collectors.toList());

    fieldBuilder.arguments(arguments);

    // Finalize the field and add it to the operation
    var field = fieldBuilder.build();
    operationBuilder.selectionSet(new SelectionSet(List.of(field)));

    return operationBuilder.build();
  }

  private SelectionSet buildSelectionSet(
      ObjectTypeDefinition type, RelDataType rowType, Document document) {
    List<Selection> selections =
        type.getFieldDefinitions().stream()
            .filter(
                f ->
                    rowType.getField(f.getName(), false, false) != null) // must be a field on table
            //        .filter(f->!f.getName().startsWith(HIDDEN_PREFIX))
            .map(fieldDef -> createSelection(fieldDef, rowType, document))
            .collect(Collectors.toList());

    return new SelectionSet(selections);
  }

  private Field createSelection(FieldDefinition fieldDef, RelDataType rowType, Document document) {
    var fieldName = fieldDef.getName();
    RelDataTypeField rowField = rowType.getField(fieldName, false, false);

    var fieldBuilder = Field.newField().name(fieldName);
    var fieldType = rowField.getType();
    var gqlFieldType = fieldDef.getType();
    var gqlComponentTypeOpt = unbox(gqlFieldType, document);
    if (gqlComponentTypeOpt.isPresent()) {
      TypeDefinition<?> gqlComponentType = gqlComponentTypeOpt.get();
      if (fieldType.getSqlTypeName() == SqlTypeName.ARRAY
          && fieldType.getComponentType().getSqlTypeName() == SqlTypeName.ROW) {
        if (gqlComponentType instanceof ObjectTypeDefinition definition) {
          fieldBuilder.selectionSet(
              buildSelectionSet(definition, fieldType.getComponentType(), document));
        }
      } else if (fieldType.getSqlTypeName() == SqlTypeName.ROW
          && gqlComponentType instanceof ObjectTypeDefinition definition) {
        fieldBuilder.selectionSet(buildSelectionSet(definition, fieldType, document));
      }
    }
    return fieldBuilder.build();
  }

  private Optional<TypeDefinition<?>> unbox(Type type, Document document) {
    if (type instanceof NonNullType nullType) {
      return unbox(nullType.getType(), document);
    }
    if (type instanceof ListType listType) {
      return unbox(listType.getType(), document);
    }

    if (type instanceof TypeName name) {
      var typeName = name.getName();
      for (Definition definition : document.getDefinitions()) {
        if (definition instanceof TypeDefinition<?> typeDefinition
            && typeDefinition.getName().equals(typeName)) {
          return Optional.of(typeDefinition);
        }
      }
      return Optional.empty();
    }
    if (type instanceof TypeDefinition<?> definition) {
      return Optional.of(definition);
    }

    return Optional.empty();
  }
}

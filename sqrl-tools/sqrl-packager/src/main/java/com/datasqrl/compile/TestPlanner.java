package com.datasqrl.compile;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.compile.TestPlan.GraphqlQuery;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.google.inject.Inject;
import graphql.language.Argument;
import graphql.language.AstPrinter;
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
import graphql.language.OperationDefinition.Operation;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.language.VariableDefinition;
import graphql.language.VariableReference;
import graphql.parser.Parser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

@AllArgsConstructor(onConstructor_ = @Inject)
public class TestPlanner {
  SqrlFramework framework;

  public TestPlan generateTestPlan(APISource source, Optional<Path> testsPath) {
    Parser parser = new Parser();
    List<GraphqlQuery> queries = new ArrayList<>();
    List<GraphqlQuery> mutations = new ArrayList<>();

    testsPath.ifPresent((p) -> {
      try (Stream<Path> paths = Files.walk(p)) {
        paths.filter(Files::isRegularFile)
            .filter(path -> path.toString().endsWith(".graphql"))
            .forEach(file -> {
              String content = null;
              try {
                content = new String(Files.readAllBytes(file));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              Document document = parser.parseDocument(content);
              extractQueriesAndMutations(document, queries, mutations, file.getFileName().toString().replace(".graphql", ""));
            });
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    Document document = parser.parseDocument(source.getSchemaDefinition());

    List<Node> queryNodes =(List<Node>)GraphqlSchemaVisitor.accept(new GqlGenerator(), document, null);

    for (Node definition : queryNodes) {
      OperationDefinition definition1 = (OperationDefinition) definition;
      queries.add(new GraphqlQuery(definition1.getName(),
          AstPrinter.printAst(definition1)));
    }
    return new TestPlan(queries, mutations, List.of());
  }

  private void extractQueriesAndMutations(Document document, List<GraphqlQuery> queries, List<GraphqlQuery> mutations, String prefix) {
    for (Definition definition : document.getDefinitions()) {
      if (definition instanceof OperationDefinition) {
        OperationDefinition operationDefinition = (OperationDefinition) definition;
        GraphqlQuery query = new GraphqlQuery(prefix, AstPrinter.printAst(operationDefinition));
        if (operationDefinition.getOperation() == Operation.QUERY) {
          queries.add(query);
        } else if (operationDefinition.getOperation() == Operation.MUTATION) {
          mutations.add(query);
        }
      }
    }
  }

  public class GqlGenerator extends GraphqlSchemaVisitor {

    @Override
    public List<Node> visitDocument(Document node, Object context) {
      List<Definition> definitions = node.getDefinitions();
      //Iterate through all queries and process them
      List<Node> queries = definitions.stream()
          .filter(definition -> definition instanceof ObjectTypeDefinition)
          .map(definition -> (ObjectTypeDefinition) definition)
          .filter(definition -> definition.getName().equals("Query"))
          .flatMap(q->processQueryDefinition(q, node).stream())
          .collect(Collectors.toList());

      return queries;
    }

    private List<Node> processQueryDefinition(ObjectTypeDefinition definition, Document document) {
      List<Node> queries = new ArrayList<>();
      for (FieldDefinition def : definition.getFieldDefinitions()) {
        //TODO: @Etienne: instead of looking the functions up in the framework, pass them into
        //The test planner and lookup the function by name. Then replace macro.isTest() with fct.getVisibility().isTest();
        SqrlTableFunction fct;
        SqrlTableMacro macro = framework.getSchema().getTableFunctions(NamePath.of(def.getName())).get(0);
        if (macro.isTest()) {
          OperationDefinition operation = processOperation(def.getName(),
              (ObjectTypeDefinition) unbox(def.getType(), document).get(),
              def.getInputValueDefinitions(),
              macro.getRowType(),
              document);
          queries.add(operation);
        }
      }
      return queries;
    }

    private OperationDefinition processOperation(String name, ObjectTypeDefinition type, List<InputValueDefinition> inputValueDefinitions,
        RelDataType rowType, Document document) {
      OperationDefinition.Builder operationBuilder = OperationDefinition.newOperationDefinition()
          .name(name)
          .operation(Operation.QUERY);

      // Add input value definitions as arguments
      List<VariableDefinition> variableDefinitions = inputValueDefinitions.stream()
          .map(input -> new VariableDefinition(input.getName(), input.getType(), null))
          .collect(Collectors.toList());

      operationBuilder.variableDefinitions(variableDefinitions);
      // Build the selection set recursively for nested fields
      SelectionSet selectionSet = buildSelectionSet(type, rowType, document);
      // Create the field for the operation
      Field.Builder fieldBuilder = Field.newField()
          .name(name)
          .selectionSet(selectionSet);

      // Connect arguments to the field
      List<Argument> arguments = inputValueDefinitions.stream()
          .map(input -> new Argument(input.getName(), new VariableReference(input.getName())))
          .collect(Collectors.toList());

      fieldBuilder.arguments(arguments);

      // Finalize the field and add it to the operation
      Field field = fieldBuilder.build();
      operationBuilder.selectionSet(new SelectionSet(List.of(field)));

      return operationBuilder.build();
    }

    @Override
    public Object visitObjectTypeDefinition(ObjectTypeDefinition node, Object context) {
      return super.visitObjectTypeDefinition(node, context);
    }
  }

  private SelectionSet buildSelectionSet(ObjectTypeDefinition type, RelDataType rowType, Document document) {
    List<Selection> selections = type.getFieldDefinitions().stream()
        .filter(f->rowType.getField(f.getName(), false, false) != null) //must be a field on table
//        .filter(f->!f.getName().startsWith(HIDDEN_PREFIX))
        .map(fieldDef -> createSelection(fieldDef, rowType, document))
        .collect(Collectors.toList());

    return new SelectionSet(selections);
  }

  private Field createSelection(FieldDefinition fieldDef, RelDataType rowType, Document document) {
    String fieldName = fieldDef.getName();
    RelDataTypeField rowField = rowType.getField(fieldName, false, false);

    Field.Builder fieldBuilder = Field.newField().name(fieldName);
    RelDataType fieldType = rowField.getType();
    Type gqlFieldType = fieldDef.getType();
    Optional<TypeDefinition<?>> gqlComponentTypeOpt = unbox(gqlFieldType, document);
    if (gqlComponentTypeOpt.isPresent()) {
      TypeDefinition<?> gqlComponentType = gqlComponentTypeOpt.get();
      if (fieldType.getSqlTypeName() == SqlTypeName.ARRAY
          && fieldType.getComponentType().getSqlTypeName() == SqlTypeName.ROW) {
        if (gqlComponentType instanceof ObjectTypeDefinition) {
          fieldBuilder.selectionSet(buildSelectionSet((ObjectTypeDefinition) gqlComponentType,
              fieldType.getComponentType(), document));
        }
      } else if (fieldType.getSqlTypeName() == SqlTypeName.ROW
          && gqlComponentType instanceof ObjectTypeDefinition) {
        fieldBuilder.selectionSet(
            buildSelectionSet((ObjectTypeDefinition) gqlComponentType, fieldType, document));
      }
    }
    return fieldBuilder.build();
  }

  private Optional<TypeDefinition<?>> unbox(Type type, Document document) {
    if (type instanceof NonNullType) {
      return unbox(((NonNullType) type).getType(), document);
    }
    if (type instanceof ListType) {
      return unbox(((ListType) type).getType(), document);
    }

    if (type instanceof TypeName) {
      String typeName = ((TypeName) type).getName();
      for (Definition definition : document.getDefinitions()) {
        if (definition instanceof TypeDefinition && ((TypeDefinition<?>) definition).getName().equals(typeName)) {
          return Optional.of((TypeDefinition<?>) definition);
        }
      }
      return Optional.empty();
    }
    if (type instanceof TypeDefinition) {
      return Optional.of((TypeDefinition<?>) type);
    }

    return Optional.empty();
  }
}

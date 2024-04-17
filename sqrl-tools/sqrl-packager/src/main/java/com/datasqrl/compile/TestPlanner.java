package com.datasqrl.compile;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.compile.TestPlan.GraphqlQuery;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.plan.queries.APISource;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class TestPlanner {
  SqrlFramework framework;

  public TestPlan generateTestPlan(APISource source, Path testsPath) {
    Parser parser = new Parser();
    List<GraphqlQuery> queries = new ArrayList<>();
    List<GraphqlQuery> mutations = new ArrayList<>();

    try (Stream<Path> paths = Files.walk(testsPath)) {
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

    Document document = parser.parseDocument(source.getSchemaDefinition());

    List<Node> queryNodes =(List<Node>)GraphqlSchemaVisitor.accept(new GqlGenerator(), document, null);

    for (Node definition : queryNodes) {
      OperationDefinition definition1 = (OperationDefinition) definition;
      queries.add(new GraphqlQuery(definition1.getName(),
          AstPrinter.printAst(definition1)));
    }
    return new TestPlan(queries, mutations);
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
      //todo: Visit the query definition
      // for each query that ends in 'Test', construct an ast node of a query of all scalar fields
      // print out the result using the code below
      List<Definition> definitions = node.getDefinitions();

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
        //Lookup function and see if it is a sqrl test macro
        // Todo: use overloaded lookup
        SqrlTableMacro macro = framework.getSchema().getTableFunctions(NamePath.of(def.getName())).get(0);
        if (macro.isTest()) {
          OperationDefinition operation = processOperation(def.getName(), (ObjectTypeDefinition) unbox(def.getType(), document), def.getInputValueDefinitions());
          queries.add(operation);
        }
      }
      return queries;
    }
    private TypeDefinition<?> unbox(Type type, Document document) {
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
            return (TypeDefinition<?>) definition;
          }
        }
        throw new IllegalArgumentException("Type " + typeName + " not found in document");
      }
      // In case the type is already a TypeDefinition, just return it
      if (type instanceof TypeDefinition) {
        return (TypeDefinition<?>) type;
      }

      throw new IllegalArgumentException("Unknown type encountered: " + type);
    }


    private OperationDefinition processOperation(String name, ObjectTypeDefinition type, List<InputValueDefinition> inputValueDefinitions) {
      // Construct the operation definition
      OperationDefinition.Builder operationBuilder = OperationDefinition.newOperationDefinition()
          .name(name)
          .operation(Operation.QUERY);

      // Add input value definitions as arguments
      List<VariableDefinition> variableDefinitions = inputValueDefinitions.stream()
          .map(input -> new VariableDefinition(input.getName(), input.getType(), null))
          .collect(Collectors.toList());

      operationBuilder.variableDefinitions(variableDefinitions);

      // Construct the selection set with all fields of the type
      List<Selection> fieldSelections = type.getFieldDefinitions().stream()
          .map(fieldDef -> Field.newField()
              .name(fieldDef.getName())
              // Assume all scalar or ID fields for simplicity, no nested objects or lists
//              .selectionSet(new SelectionSet(new ArrayList<>()))
              .build())
          .collect(Collectors.toList());

      SelectionSet selectionSet = new SelectionSet(fieldSelections);

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
}

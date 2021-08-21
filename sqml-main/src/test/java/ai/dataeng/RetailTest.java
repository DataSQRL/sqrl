package ai.dataeng;

import ai.dataeng.sqml.GraphqlSchemaBuilder;
import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Analyzer;
import ai.dataeng.sqml.execution.Bundle;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.function.PostgresFunctions;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.schema.SchemaProvider;
import ai.dataeng.sqml.tree.Script;
import graphql.schema.GraphQLSchema;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

public class RetailTest {

  @Test
  public void test() throws IOException {
    Bundle bundle = new Bundle.Builder()
        .setMainScript("../sqml-examples/retail/c360/c360.sqml")
        .setPath(Paths.get("../sqml-examples/retail/"))
        .build();
    SqmlParser parser = SqmlParser.newSqmlParser();
    Script script = parser.parse(new String(
        Files.readAllBytes(Paths.get(bundle.getMainScriptName()))));

    Metadata metadata = new Metadata(FunctionProvider.newFunctionProvider()
        .function(PostgresFunctions.SqmlSystemFunctions).build(), null,
        null, null, bundle, mockSchemaProvider());
    Analysis analysis = Analyzer.analyze(script, metadata);

    System.out.println(analysis);

    GraphQLSchema graphqlSchema = GraphqlSchemaBuilder
        .newGraphqlSchema()
        .script(script)
        .analysis(analysis)
        .build();


    //1. Create c360 SQML bundle
    //2. Resolve it in analyzer
    //3. Resolve a 'model'
    //4. Work on a major component
    // - Graphql query parser
    // - Remove DAG
    // - Remove schema inferencer
    // - Remove graphql visitor, move to sqml model visitor
    // - Add wrapper to scalar fields

  }

  private SchemaProvider mockSchemaProvider() {
    return null;
  }
}


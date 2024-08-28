//package com.datasqrl.graphql.inference;
//
//import static org.mockito.Mockito.doNothing;
//import static org.mockito.Mockito.mock;
//
//import com.datasqrl.AbstractEngineIT;
//import com.datasqrl.IntegrationTestSettings;
//import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
//import com.datasqrl.IntegrationTestSettings.LogEngine;
//import com.datasqrl.IntegrationTestSettings.StreamEngine;
//import com.datasqrl.canonicalizer.NameCanonicalizer;
//import com.datasqrl.error.ErrorCollector;
//import com.datasqrl.graphql.APIConnectorManager;
//import com.datasqrl.graphql.GraphqlSchemaParser;
//import com.datasqrl.loaders.ModuleLoader;
//import com.datasqrl.plan.queries.APIMutation;
//import com.datasqrl.plan.queries.APISource;
//import com.datasqrl.plan.queries.APISourceImpl;
//import com.datasqrl.io.schema.flexible.converters.FlexibleSchemaExporter;
//import com.datasqrl.io.schema.flexible.FlexibleTableSchemaHolder;
//import com.datasqrl.util.SnapshotTest;
//import com.datasqrl.util.SnapshotTest.Snapshot;
//import com.google.common.io.Resources;
//import java.nio.charset.Charset;
//import java.util.Optional;
//import lombok.SneakyThrows;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.TestInfo;
//import org.mockito.ArgumentCaptor;
//
//@Disabled
//public class GraphQLMutationExtractionTest extends AbstractEngineIT {
//
//  private Snapshot snapshot;
//
//  @BeforeEach
//  public void before(TestInfo testInfo) {
//    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
//  }
//
//  @SneakyThrows
//  @Test
//  public void testMutationProcessing() {
//    initialize(IntegrationTestSettings.builder()
//        .stream(StreamEngine.FLINK).database(DatabaseEngine.POSTGRES)
//        .log(LogEngine.KAFKA).build(), null, Optional.empty());
//
//    APIConnectorManager apiManager = mock(APIConnectorManager.class);
//    ArgumentCaptor<APIMutation> mutationCaptor = ArgumentCaptor.forClass(APIMutation.class);
//
//    // Mock the addMutation() method
//    doNothing().when(apiManager).addMutation(mutationCaptor.capture());
//
//    GraphQLMutationExtraction preprocessor = new GraphQLMutationExtraction(
//        injector.getInstance(GraphqlSchemaParser.class),
//        injector.getInstance(RelDataTypeFactory.class),
//        injector.getInstance(NameCanonicalizer.class),
//        injector.getInstance(ModuleLoader.class),
//        injector.getInstance(ErrorCollector.class),
//        apiManager);
//
//    APISource gql = APISourceImpl.of(Resources.toString(Resources.getResource("graphql/schema.graphqls"), Charset.defaultCharset()));
//
//    preprocessor.analyze(gql);
//
//    FlexibleSchemaExporter schemaGenerator = new FlexibleSchemaExporter();
//
//    for (APIMutation capturedMutation : mutationCaptor.getAllValues()) {
//      // Process each captured mutation
//      // For example, convert the schema of each mutation and add to the snapshot
//      FlexibleTableSchemaHolder flexSchema = schemaGenerator.convert(capturedMutation.getSchema());
//      snapshot.addContent(flexSchema.getDefinition(), capturedMutation.toString());
//    }
//
//    snapshot.createOrValidate();
//  }
//
//}

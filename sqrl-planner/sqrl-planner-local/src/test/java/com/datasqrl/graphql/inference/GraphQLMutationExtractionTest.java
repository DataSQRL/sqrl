package com.datasqrl.graphql.inference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.local.analyze.MockAPIConnectorManager;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.schema.converters.FlexibleSchemaExporter;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.google.common.io.Resources;
import java.nio.charset.Charset;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;

public class GraphQLMutationExtractionTest {

  private Snapshot snapshot;

  @BeforeEach
  public void before(TestInfo testInfo) {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @SneakyThrows
  @Test
  public void testMutationProcessing() {
    GraphQLMutationExtraction preprocessor = new GraphQLMutationExtraction(TypeFactory.getTypeFactory(), NameCanonicalizer.SYSTEM);
    APIConnectorManager apiManager = mock(APIConnectorManager.class);

    ArgumentCaptor<APIMutation> mutationCaptor = ArgumentCaptor.forClass(APIMutation.class);

    // Mock the addMutation() method
    doNothing().when(apiManager).addMutation(mutationCaptor.capture());

    APISource gql = APISource.of(Resources.toString(Resources.getResource("graphql/schema.graphqls"), Charset.defaultCharset()));

    preprocessor.analyze(gql, apiManager);

    FlexibleSchemaExporter schemaGenerator = new FlexibleSchemaExporter();

    for (APIMutation capturedMutation : mutationCaptor.getAllValues()) {
      // Process each captured mutation
      // For example, convert the schema of each mutation and add to the snapshot
      FlexibleTableSchemaHolder flexSchema = schemaGenerator.convert(capturedMutation.getSchema());
      snapshot.addContent(flexSchema.getDefinition(), capturedMutation.toString());
    }

    snapshot.createOrValidate();
  }

}

package com.datasqrl.graphql.inference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.packager.preprocess.Preprocessor.ProcessorContext;
import com.datasqrl.plan.local.analyze.MockAPIConnectorManager;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.schema.TableSchemaExporterFactory;
import com.datasqrl.schema.TypeFactory;
import com.datasqrl.schema.converters.FlexibleSchemaExporter;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.datasqrl.schema.input.external.SchemaExport;
import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    MockAPIConnectorManager apiManager = new MockAPIConnectorManager();
    APISource gql = APISource.of(Resources.toString(Resources.getResource("graphql/schema.graphqls"), Charset.defaultCharset()));

    preprocessor.analyze(gql, apiManager);

    FlexibleSchemaExporter schemaGenerator = new FlexibleSchemaExporter();

    apiManager.getMutations().forEach(mut -> {
      FlexibleTableSchemaHolder flexSchema = schemaGenerator.convert(mut.getSchema());
      snapshot.addContent(flexSchema.getDefinition(),mut.toString());
    });
    snapshot.createOrValidate();
  }

}

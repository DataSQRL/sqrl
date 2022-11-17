package ai.datasqrl.graphql.inference;

import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.util.data.Retail;
import ai.datasqrl.util.data.Retail.RetailScriptNames;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaInferenceModelTest extends AbstractSchemaInferenceModelTest {
  private Retail example = Retail.INSTANCE;

  @BeforeEach
  public void setup() throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory());
  }

  @Test
  public void testC360Inference() {
    Pair<InferredSchema, List<APIQuery>> result = inferSchemaAndQueries(example.getScript(RetailScriptNames.FULL),
            Path.of("src/test/resources/c360bundle/schema.full.graphqls"));
    assertEquals(64, result.getKey().getQuery().getFields().size());
    assertEquals(444, result.getValue().size());
  }
}
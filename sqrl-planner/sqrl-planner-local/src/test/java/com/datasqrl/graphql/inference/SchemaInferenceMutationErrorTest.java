package com.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.IntegrationTestSettings.LogEngine;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class SchemaInferenceMutationErrorTest extends AbstractSchemaInferenceModelTest {

  private Snapshot snapshot;
  public static final String IMPORT_SCRIPT = "IMPORT ecommerce-data.Orders;\n"
      + "IMPORT ecommerce-data.Product;\n";

  @BeforeEach
  protected void initialize(TestInfo testInfo) {
    initialize(IntegrationTestSettings.builder()
        .log(LogEngine.KAFKA)
        .build(), null, Optional.empty());
    snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    plan(IMPORT_SCRIPT);
  }

  protected void validateErrorsAndAddContent() {
    if (!errors.hasErrors()) {
      fail("Errors expected:");
    }

    snapshot.addContent(ErrorPrinter.prettyPrint(errors.getErrors()));
    snapshot.createOrValidate();
  }

  @Test
  public void mustHaveQueryType() {
    super.inferSchemaModelQueries("type Orders {\n\tid: Int\n}\n", framework, pipeline, errors);
    assertFalse(errors.hasErrorsWarningsOrNotices());
  }

  @Test
  public void inputMustBeNonnull() {
    super.inferSchemaModelQueries("type Orders {\n" +
        "\t_uuid: String\n" +
        "}\n"
        + "input OrderInput {\n" +
        "\tid: Int!" +
        "\n}" +
        "\n"
        + "type Mutation {\n" +
        "\taddOrder(input: OrderInput): Orders\n" +
        "}\n"
        + "type Query {\n" +
        "\torders: Orders" +
        "\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void validSubscriptionArrayTest() {
    plan(
      "TestOutput := STREAM ON ADD AS SELECT COLLECT(productid) AS id FROM Product;\n");
    inferSchemaModelQueries("type Query {\n\torders: [Orders]\n}\n"
        + "type Subscription {\n\ttestOutput: TestOutput\n}\n"
        + "input TestInput {\n\tid: [Int!]!\n}\n"
        + "type TestOutput {\n\tid: [Int!]!\n}\n"
        + "type Orders {\n\tid: [Int!]!\n}\n", framework, pipeline, errors
    );

    assertFalse(errors.hasErrorsWarningsOrNotices());
  }

  @Test
  public void nonMatchingInputAndOutputTypes() {
    super.inferSchemaModelQueries("type Orders {\n\tid: Int\n}\n"
        + "input OrderInput {\n\tid: String!\n}\n"
        + "type Mutation {\n\taddOrder(input: OrderInput!): Orders\n}"
        + "type Query {\n\torders: Orders\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void mutationNotASink() {
   super.inferSchemaModelQueries("type Orders {\n\tid: Int!\n}\n"
        + "input OrderInput {\n\tid: Int!\n}\n"
        + "type Mutation {\n\taddOrder(input: OrderInput!): Orders\n}"
        + "type Query {\n\torders: Orders\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }
}
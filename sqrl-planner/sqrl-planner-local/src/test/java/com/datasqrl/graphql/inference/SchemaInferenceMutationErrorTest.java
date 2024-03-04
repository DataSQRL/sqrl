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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class SchemaInferenceMutationErrorTest extends AbstractSchemaInferenceModelTest {

  private Snapshot snapshot;
  public static final String IMPORT_SCRIPT = "IMPORT ecommerce-data.Orders TIMESTAMP _ingest_time;\n"
      + "IMPORT ecommerce-data.Product TIMESTAMP _ingest_time;\n";

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
    super.inferSchemaModelQueriesErr("type Orders {\n  id: Int\n}\n", framework, errors);
    assertTrue(errors.hasErrorsWarningsOrNotices());
  }

  @Test
  public void inputMustBeNonnull() {
    super.inferSchemaModelQueriesErr("type Orders {\n" +
        "  _uuid: String\n" +
        "}\n"
        + "input OrderInput {\n" +
        "  id: Int!" +
        "\n}" +
        "\n"
        + "type Mutation {\n" +
        "  addOrder(input: OrderInput): Orders\n" +
        "}\n"
        + "type Query {\n" +
        "  orders: Orders" +
        "\n}", framework, errors);
    validateErrorsAndAddContent();
  }

  @Test
  @Disabled //disabled, removal of stream keyword in 0.5
  public void validSubscriptionArrayTest() {
    plan("TestOutput := STREAM ON ADD AS SELECT COLLECT(productid) AS id FROM Product;\n");
    inferSchemaModelQueries("type Query {\n  orders: [Orders]\n}\n"
        + "type Subscription {\n  testOutput: TestOutput\n}\n"
        + "input TestInput {\n  id: [Int!]!\n}\n"
        + "type TestOutput {\n  id: [Int!]!\n}\n"
        + "type Orders {\n  id: [Int!]!\n}\n", framework, errors
    );

    assertFalse(errors.hasErrorsWarningsOrNotices());
  }

  @Test
  public void nonMatchingInputAndOutputTypes() {
    super.inferSchemaModelQueriesErr("type Orders {\n  id: Int\n}\n"
        + "input OrderInput {\n  id: String!\n}\n"
        + "type Mutation {\n  addOrder(input: OrderInput!): Orders\n}"
        + "type Query {\n  orders: Orders\n}", framework, errors);
    validateErrorsAndAddContent();
  }

//  @Test
//  public void mutationNotASink() {
//   super.inferSchemaModelQueriesErr("type Orders {\n  id: Int!\n}\n"
//        + "input OrderInput {\n  id: Int!\n}\n"
//        + "type Mutation {\n  addOrder(input: OrderInput!): Orders\n}"
//        + "type Query {\n  orders: Orders\n}", framework, errors);
//    validateErrorsAndAddContent();
//  }
}
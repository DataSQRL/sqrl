package com.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.IntegrationTestSettings;
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
      + "IMPORT ecommerce-data.Product;";
  @BeforeEach
  protected void initialize(TestInfo testInfo) {
    initialize(IntegrationTestSettings.builder()
        .build(), null, Optional.empty());
    snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    ns = plan(IMPORT_SCRIPT);
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
    super.inferSchemaModelQueries(this.planner,"type Orders {\n\t_uuid: String\n}\n"
        + "input OrderInput {\n\tid: Int!\n}\n"
        + "type Mutation {\n\taddOrder(input: OrderInput): Orders\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void inputMustBeNonnull() {
    super.inferSchemaModelQueries(this.planner,"type Orders {\n" +
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
        "\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void nonMatchingInputAndOutputTypes() {
    super.inferSchemaModelQueries(this.planner,"type Orders {\n\tid: Int\n}\n"
        + "input OrderInput {\n\tid: String!\n}\n"
        + "type Mutation {\n\taddOrder(input: OrderInput!): Orders\n}"
        + "type Query {\n\torders: Orders\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void mutationNotASink() {
   super.inferSchemaModelQueries(this.planner,"type Orders {\n\tid: Int!\n}\n"
        + "input OrderInput {\n\tid: Int!\n}\n"
        + "type Mutation {\n\taddOrder(input: OrderInput!): Orders\n}"
        + "type Query {\n\torders: Orders\n}");
    validateErrorsAndAddContent();
  }
}
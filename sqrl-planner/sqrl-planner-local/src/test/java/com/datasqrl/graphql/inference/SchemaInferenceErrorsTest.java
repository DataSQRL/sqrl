package com.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.graphql.generate.SchemaGenerator;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.SnapshotTest.Snapshot;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import graphql.schema.idl.SchemaPrinter.Options;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class SchemaInferenceErrorsTest extends AbstractSchemaInferenceModelTest {

  private static final String IMPORT_SCRIPT = "IMPORT ecommerce-data.Orders;\n"
      + "IMPORT ecommerce-data.Product;";
  private Snapshot snapshot;

  @BeforeEach
  protected void initialize(TestInfo testInfo) {
    initialize(IntegrationTestSettings.builder()
        .build(), null, Optional.empty());
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
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
  public void generateSchemaTest() {
    SqrlSchemaForInference sqrlSchemaForInference = new SqrlSchemaForInference(planner.getFramework().getSchema());

    GraphQLSchema gqlSchema = new SchemaGenerator().generate(sqrlSchemaForInference, true);

    if (errors.hasErrors()) {
      fail("Exception thrown");
    }
    snapshot.addContent(new SchemaPrinter(Options.defaultOptions()
        .includeDirectives(false)).print(gqlSchema));
    snapshot.createOrValidate();
  }

  @Test
  public void invalidFieldTest() {
    inferSchemaModelQueries(planner,
            "type Orders {\n\tmissingField: String\n}\n"
                + "type Query {\n\torders: Orders\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void invalidTableTest() {
    inferSchemaModelQueries(planner, "type Orders {\n\tmissingField: String\n}\n"
        + "type Query {\n\torders: DOESNOTEXIST\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void missingNestedTable() {
    inferSchemaModelQueries(planner,
        "type Orders {\n\tentries: MISSING\n}\n"
            + "type Query {\n\torders: Orders\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void renameNestedTableTest() {
    inferSchemaModelQueries(planner,
        "type Orders {\n\tentries: OrderEntries\n}\n"
            + "type OrderEntries {\n\tproductid: Int\n}\n"
            + "type Query {\n\torders: Orders\n}");
    if (errors.hasErrors()) {
      fail("No errors expected");
    }
  }

  @Test
  public void structuralObjectTest() {
    inferSchemaModelQueries(planner, ""
        + "type Orders {\n\t_uuid: String, entries: Entries\n}\n"
        + "type Entries {\n\tinvalidField: String\n}\n"
        + "type Query {\n\torders: Orders\n\tproduct: Orders\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void arrayType() {
    inferSchemaModelQueries(planner, "type Orders {\n\t_uuid: String\n}\n"
        + "type Query {\n\torders: [[Orders]]\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void array2Type() {
    inferSchemaModelQueries(planner, "type Orders {\n\t_uuid: String\n}\n"
        + "type Query {\n\torders: [[Orders!]]!\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void array3Type() {
    inferSchemaModelQueries(planner, "type Orders {\n\t_uuid: String\n\tentries: [[Entries]]\n}\n"
        + "type Entries {\n\tproductid: String\n}\n"
        + "type Query {\n\torders: Orders\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void array4Type() {
    inferSchemaModelQueries(planner, "type Orders {\n\t_uuid: String\n\tentries: [[Entries!]!]!\n}\n"
        + "type Entries {\n\tproductid: String\n}\n"
        + "type Query {\n\torders: Orders\n}");
    validateErrorsAndAddContent();
  }

  @Test
  public void tooManyFields() {
    inferSchemaModelQueries(planner, "type Orders {\n  _uuid: String\n  x: Int!\n}\n"
        + "type Query {\n  orders: Orders\n}");
    validateErrorsAndAddContent();
  }
}
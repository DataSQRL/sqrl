package com.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.graphql.generate.GraphqlSchemaFactory;
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
  public void generateSchemaTest() {
    GraphQLSchema gqlSchema = new GraphqlSchemaFactory(framework.getSchema(), true).generate();

    if (errors.hasErrors()) {
      fail("Exception thrown");
    }
    snapshot.addContent(new SchemaPrinter(Options.defaultOptions()
        .includeDirectives(false)).print(gqlSchema));
    snapshot.createOrValidate();
  }

  @Test
  public void invalidFieldTest() {
    inferSchemaModelQueries(
            "type Orders {\n\tmissingField: String\n}\n"
                + "type Query {\n\torders: Orders\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void invalidTableTest() {
    inferSchemaModelQueries( "type Orders {\n\tmissingField: String\n}\n"
        + "type Query {\n\torders: DOESNOTEXIST\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void missingNestedTable() {
    inferSchemaModelQueries(
        "type Orders {\n\tentries: MISSING\n}\n"
            + "type Query {\n\torders: Orders\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void renameNestedTableTest() {
    inferSchemaModelQueries(
        "type Orders {\n\tentries: OrderEntries\n}\n"
            + "type OrderEntries {\n\tproductid: Int\n}\n"
            + "type Query {\n\torders: Orders\n}", framework, pipeline, errors);
    if (errors.hasErrors()) {
      fail("No errors expected");
    }
  }

  @Test
  public void structuralObjectTest() {
    inferSchemaModelQueries(""
        + "type Orders {\n\t_uuid: String, entries: Entries\n}\n"
        + "type Entries {\n\tinvalidField: String\n}\n"
        + "type Query {\n\torders: Orders\n\tproduct: Orders\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void arrayType() {
    inferSchemaModelQueries("type Orders {\n\t_uuid: String\n}\n"
        + "type Query {\n\torders: [[Orders]]\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void array2Type() {
    inferSchemaModelQueries("type Orders {\n\t_uuid: String\n}\n"
        + "type Query {\n\torders: [[Orders!]]!\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void array3Type() {
    inferSchemaModelQueries("type Orders {\n\t_uuid: String\n\tentries: [[Entries]]\n}\n"
        + "type Entries {\n\tproductid: String\n}\n"
        + "type Query {\n\torders: Orders\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void array4Type() {
    inferSchemaModelQueries("type Orders {\n\t_uuid: String\n\tentries: [[Entries!]!]!\n}\n"
        + "type Entries {\n\tproductid: String\n}\n"
        + "type Query {\n\torders: Orders\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void tooManyFields() {
    inferSchemaModelQueries("type Orders {\n  _uuid: String\n  x: Int!\n}\n"
        + "type Query {\n  orders: Orders\n}", framework, pipeline, errors);
    validateErrorsAndAddContent();
  }
}
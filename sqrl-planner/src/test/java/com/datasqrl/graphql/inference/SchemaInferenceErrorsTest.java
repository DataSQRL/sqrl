package com.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.graphql.generate.GraphqlSchemaFactory;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.SnapshotTest.Snapshot;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import graphql.schema.idl.SchemaPrinter.Options;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Disabled
class SchemaInferenceErrorsTest extends AbstractSchemaInferenceModelTest {

  private static final String IMPORT_SCRIPT =
      "IMPORT ecommerce-data.Orders;\n" + "IMPORT ecommerce-data.Product;";
  private Snapshot snapshot;

  @BeforeEach
  protected void initialize(TestInfo testInfo) {
    initialize(IntegrationTestSettings.builder().build(), null, Optional.empty());
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
    GraphQLSchema gqlSchema = injector.getInstance(GraphqlSchemaFactory.class)
        .generate(ExecutionGoal.COMPILE);

    if (errors.hasErrors()) {
      fail("Exception thrown");
    }
    snapshot.addContent(
        new SchemaPrinter(Options.defaultOptions().includeDirectives(false)).print(gqlSchema));
    snapshot.createOrValidate();
  }

  @Test
  public void invalidFieldTest() {
    inferSchemaModelQueriesErr(
        "type Orders {\n  missingField: String\n}\n" + "type Query {\n  orders: Orders\n}",
        framework, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void invalidTableTest() {
    inferSchemaModelQueriesErr(
        "type Orders {\n  missingField: String\n}\n" + "type Query {\n  orders: DOESNOTEXIST\n}",
        framework, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void missingNestedTable() {
    inferSchemaModelQueriesErr(
        "type Orders {\n  entries: MISSING\n}\n" + "type Query {\n  orders: Orders\n}", framework,
        errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void renameNestedTableTest() {
    inferSchemaModelQueries(
        "type Orders {\n  entries: OrderEntries\n}\n" + "type OrderEntries {\n  productid: Int\n}\n"
            + "type Query {\n  orders: Orders\n}", framework, errors);
    if (errors.hasErrors()) {
      fail("No errors expected");
    }
  }

  @Test
  public void structuralObjectTest() {
    inferSchemaModelQueriesErr("type Orders {\n  _uuid: String, entries: Entries\n}\n"
        + "type Entries {\n  invalidField: String\n}\n"
        + "type Query {\n  orders: Orders\n  product: Orders\n}", framework, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void arrayType() {
    inferSchemaModelQueriesErr(
        "type Orders {\n  _uuid: String\n}\n" + "type Query {\n  orders: [[Orders]]\n}", framework,
        errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void array2Type() {
    inferSchemaModelQueriesErr(
        "type Orders {\n  _uuid: String\n}\n" + "type Query {\n  orders: [[Orders!]]!\n}",
        framework, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void array3Type() {
    inferSchemaModelQueriesErr("type Orders {\n  _uuid: String\n  entries: [[Entries]]\n}\n"
            + "type Entries {\n  productid: String\n}\n" + "type Query {\n  orders: Orders\n}",
        framework, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void array4Type() {
    inferSchemaModelQueriesErr("type Orders {\n  _uuid: String\n  entries: [[Entries!]!]!\n}\n"
            + "type Entries {\n  productid: String\n}\n" + "type Query {\n  orders: Orders\n}",
        framework, errors);
    validateErrorsAndAddContent();
  }

  @Test
  public void tooManyFields() {
    inferSchemaModelQueriesErr(
        "type Orders {\n  _uuid: String\n  x: Int!\n}\n" + "type Query {\n  orders: Orders\n}",
        framework, errors);
    validateErrorsAndAddContent();
  }
}
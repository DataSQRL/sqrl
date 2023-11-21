/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import com.datasqrl.IntegrationTestSettings;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;

@Slf4j
public class SchemaGeneratorTest extends AbstractSchemaGeneratorTest {

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), (Path) null);
    super.setup(testInfo);
  }

  @Test
  public void testImport() {
    snapshotTest("IMPORT ecommerce-data.Product;");
  }

  @Test
  public void testImportWithDifferentCapitalization() {
    snapshotTest("IMPORT ecommerce-data.product;");
  }

  @Test
  public void testAliased() {
    snapshotTest("IMPORT ecommerce-data.Product AS pRoDuCt;");
  }

  @Test
  public void testImportNested() {
    snapshotTest("IMPORT ecommerce-data.Orders;");
  }

  @Test
  public void testImportNestedNoArgs() {
    snapshotTest("IMPORT ecommerce-data.Orders;", false);
  }

  @Test
  public void testJson() {
    snapshotTest("IMPORT ecommerce-data.Product;\n"
        + "IMPORT json.*;\n"
        + "Product.json := toJson('{}');");
  }
}
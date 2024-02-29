/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import com.datasqrl.IntegrationTestSettings;
import java.nio.file.Path;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;

@Slf4j
public class GraphqlSchemaFactoryTest extends AbstractGraphqlSchemaFactoryTest {

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), (Path) null, Optional.empty());
    super.setup(testInfo);
  }

  @Test
  public void testImport() {
    snapshotTest("IMPORT ecommerce-data.Product TIMESTAMP _ingest_time;");
  }

  @Test
  public void testImportWithDifferentCapitalization() {
    snapshotTest("IMPORT ecommerce-data.product TIMESTAMP _ingest_time;");
  }

  @Test
  public void testAliased() {
    snapshotTest("IMPORT ecommerce-data.Product AS pRoDuCt;");
  }

  @Test
  public void testPrimaryKey() {
    snapshotTest("IMPORT ecommerce-data.Product TIMESTAMP _ingest_time;\n"
        + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;");
  }

  @Test
  public void testNestedPrimaryKey() {
    snapshotTest("IMPORT ecommerce-data.product TIMESTAMP _ingest_time;\n"
        + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
        + "Product.nested := SELECT p.productid, count(*) cnt FROM @ JOIN Product p GROUP BY p.productid;");
  }

  @Test
  public void testNestedTable() {
    snapshotTest("IMPORT ecommerce-data.Orders TIMESTAMP _ingest_time;\n"
        + "Orders := DISTINCT Orders ON id ORDER BY _ingest_time DESC;\n");
  }

  @Test
  public void testNestedNoPrimaryKey() {
    snapshotTest("IMPORT ecommerce-data.product TIMESTAMP _ingest_time;\n"
        + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
        + "Product.nested := SELECT p.productid, p.name FROM @ JOIN Product p");
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
    snapshotTest("IMPORT ecommerce-data.Product TIMESTAMP _ingest_time;\n"
        + "IMPORT json.*;\n"
        + "Product.json := toJson('{}');");
  }
}
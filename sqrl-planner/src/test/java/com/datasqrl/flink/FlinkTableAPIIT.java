/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.AbstractPhysicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.flink.FlinkEngineConfiguration;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.engine.stream.flink.LocalFlinkStreamEngineImpl;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.util.StreamInputPreparer;
import com.datasqrl.io.util.StreamInputPreparerImpl;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.datasqrl.schema.input.SchemaValidator;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.data.Retail;
import lombok.SneakyThrows;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FlinkTableAPIIT extends AbstractPhysicalSQRLIT {

  private final TestDataset example = Retail.INSTANCE;


  @BeforeEach
  public void setup() {
    initialize(IntegrationTestSettings.builder()
            .stream(IntegrationTestSettings.StreamEngine.FLINK)
            .database(IntegrationTestSettings.DatabaseEngine.POSTGRES).build(),
        example.getRootPackageDirectory());
  }

  @SneakyThrows
  @Test
  public void testFlinkTableAPIIntegration() {

    TableSource tblSource = loadTable(NamePath.of("ecommerce-data", "Orders"));

    LocalFlinkStreamEngineImpl flink = new LocalFlinkStreamEngineImpl(
        new FlinkEngineConfiguration());
    FlinkStreamEngine.Builder streamBuilder = flink.createJob();
    StreamInputPreparer streamPreparer = new StreamInputPreparerImpl();

    StreamHolder<SourceRecord.Raw> stream = streamPreparer.getRawInput(tblSource, streamBuilder,
        ErrorPrefix.INPUT_DATA);
    SchemaValidator schemaValidator = new SchemaValidator(tblSource.getSchema(),
        SchemaAdjustmentSettings.DEFAULT, tblSource.getDigest());
    StreamHolder<SourceRecord.Named> validate = stream.mapWithError(schemaValidator.getFunction(),
        "schema", ErrorPrefix.INPUT_DATA, SourceRecord.Named.class);
    streamBuilder.addAsTable(validate, tblSource.getSchema(), "orders");

    StreamTableEnvironment tEnv = streamBuilder.getTableEnvironment();

//    Table tableShredding = tEnv.sqlQuery("SELECT  o._uuid, items._idx, o.customerid, items.discount, items.quantity, items.productid, items.unit_price \n" +
//            "FROM orders o CROSS JOIN UNNEST(o.entries) AS items");
//    tEnv.toChangelogStream(tableShredding).print();

//    Table tableSelectUnnest = tEnv.sqlQuery("SELECT o.id, (SELECT SUM(items.unit_price*items.quantity-items.discount) FROM " //(Table o) CROSS JOIN
//            + "UNNEST(o.entries) AS items) AS total FROM orders o");
//    tEnv.toChangelogStream(tableSelectUnnest).print();

    Table tableNest = tEnv.sqlQuery("SELECT COALESCE(_uuid, '') FROM orders"
        + "");
    tEnv.toChangelogStream(tableNest).print();

    streamBuilder.setJobType(FlinkStreamEngine.JobType.SCRIPT);
    streamBuilder.build().execute("test");
  }
}
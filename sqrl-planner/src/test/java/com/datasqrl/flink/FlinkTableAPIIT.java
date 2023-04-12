/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.AbstractPhysicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.flink.AbstractFlinkStreamEngine;
import com.datasqrl.engine.stream.flink.FlinkEngineConfiguration;
import com.datasqrl.engine.stream.flink.FlinkStreamBuilder;
import com.datasqrl.engine.stream.flink.FlinkStreamHolder;
import com.datasqrl.engine.stream.flink.schema.FlinkRowConstructor;
import com.datasqrl.engine.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.engine.stream.flink.schema.UniversalTable2FlinkSchema;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.stats.DefaultSchemaGenerator;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.util.StreamInputPreparer;
import com.datasqrl.io.util.StreamInputPreparerImpl;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.FlexibleSchemaRowMapper;
import com.datasqrl.schema.converters.RowMapper;
import com.datasqrl.schema.input.DefaultSchemaValidator;
import com.datasqrl.schema.input.InputTableSchema;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.data.Retail;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FlinkTableAPIIT extends AbstractPhysicalSQRLIT {

  private final TestDataset example = Retail.INSTANCE;


  @BeforeEach
  public void setup() {
    initialize(IntegrationTestSettings.builder()
            .stream(IntegrationTestSettings.StreamEngine.FLINK)
            .database(IntegrationTestSettings.DatabaseEngine.POSTGRES).build(),
        (Path) null);
  }

  @SneakyThrows
  @Test
  public void testFlinkTableAPIIntegration() {

    TableSource tblSource = loadTable(NamePath.of("ecommerce-data", "Orders"));

    AbstractFlinkStreamEngine flink = FlinkEngineConfiguration.builder().build().initialize(errors);
    FlinkStreamBuilder streamBuilder = flink.createJob();
    StreamInputPreparer streamPreparer = new StreamInputPreparerImpl();


    StreamHolder<SourceRecord.Raw> stream = streamPreparer.getRawInput(tblSource, streamBuilder, ErrorPrefix.INPUT_DATA);
    DefaultSchemaValidator schemaValidator = new DefaultSchemaValidator(tblSource.getSchema(),
        SchemaAdjustmentSettings.DEFAULT, NameCanonicalizer.SYSTEM,
        new DefaultSchemaGenerator(SchemaAdjustmentSettings.DEFAULT));
    FlinkStreamHolder<SourceRecord.Named> flinkStream = (FlinkStreamHolder)stream.mapWithError(schemaValidator.getFunction(),
        ErrorPrefix.INPUT_DATA, SourceRecord.Named.class);

    InputTableSchema schema = tblSource.getSchema();
    //TODO: error handling when mapping doesn't work?
    UniversalTable universalTable = schema.getSchema().createUniversalTable(schema.isHasSourceTimestamp(),
        Optional.empty());
    Schema flinkSchema = new UniversalTable2FlinkSchema().convertSchema(universalTable);
    TypeInformation typeInformation = new FlinkTypeInfoSchemaGenerator()
        .convertSchema(universalTable);
    RowMapper rowMapper = schema.getSchema().getRowMapper(FlinkRowConstructor.INSTANCE, schema.isHasSourceTimestamp());
    DataStream rows = flinkStream.getStream()
        .map(rowMapper::apply, typeInformation);

    StreamTableEnvironment tEnv = streamBuilder.getTableEnvironment();

    tEnv.createTemporaryView("orders", rows, flinkSchema);

//    Table tableShredding = tEnv.sqlQuery("SELECT  o._uuid, items._idx, o.customerid, items.discount, items.quantity, items.productid, items.unit_price \n" +
//            "FROM orders o CROSS JOIN UNNEST(o.entries) AS items");
//    tEnv.toChangelogStream(tableShredding).print();

//    Table tableSelectUnnest = tEnv.sqlQuery("SELECT o.id, (SELECT SUM(items.unit_price*items.quantity-items.discount) FROM " //(Table o) CROSS JOIN
//            + "UNNEST(o.entries) AS items) AS total FROM orders o");
//    tEnv.toChangelogStream(tableSelectUnnest).print();

//    Table tableNest = tEnv.sqlQuery("SELECT COALESCE(_uuid, '') FROM orders"
//        + "");
//    tEnv.toChangelogStream(tableNest).print();


    Table aggTest = tEnv.sqlQuery("SELECT customerid, count(1) AS num FROM orders GROUP BY customerid");

//    tEnv.toChangelogStream(aggTest).map(r -> {
//      System.out.println("Printing:" + r);
//      return r;
//    }).print();

    DataStream<Row> tableStream = tEnv.toChangelogStream(aggTest).filter(r -> {
      System.out.println("xtprint:" + r);
      return r.getKind()==RowKind.INSERT;
    }).process(new AugmentStream(), Types.ROW_NAMED(new String[]{"customerid", "num"}, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));

    tEnv.createTemporaryView("testStreamTable", tableStream, aggTest.getSchema().toSchema());

    StatementSet s = tEnv.createStatementSet();

    Table streamTable = tEnv.from("testStreamTable"); //tEnv.fromChangelogStream(tableStream, aggTest.getSchema().toSchema());

    TableDescriptor sink = TableDescriptor.forConnector("print")
        .option("print-identifier", "xtprint").build();
    s.addInsert(sink, streamTable);
    TableResult res = s.execute();

    res.await();

//    streamBuilder.build().execute("test");
  }

  public static class AugmentStream extends ProcessFunction<Row, Row> {

    @Override
    public void processElement(Row row, ProcessFunction<Row, Row>.Context context,
        Collector<Row> collector) throws Exception {
      System.out.println("Timestamp: " + context.timestamp());
      Object[] data = new Object[row.getArity()];
      for (int i = 0; i < row.getArity(); i++) {
        data[i] = row.getField(i);
      }
      collector.collect(Row.ofKind(RowKind.INSERT, data));
    }
  }
}
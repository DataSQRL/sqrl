/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.AbstractPhysicalSQRLIT;
import com.datasqrl.FlinkRowConstructor;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.flink.AbstractFlinkStreamEngine;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.engine.stream.flink.FlinkStreamBuilder;
import com.datasqrl.engine.stream.flink.FlinkStreamHolder;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.util.StreamInputPreparer;
import com.datasqrl.io.util.StreamInputPreparerImpl;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.engine.stream.RowMapper;
import com.datasqrl.schema.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.schema.converters.UniversalTable2FlinkSchema;
import com.datasqrl.schema.input.FlexibleSchemaValidator;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.data.Retail;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.calcite.rel.type.RelDataType;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class FlinkTableAPIIT extends AbstractPhysicalSQRLIT {

  private final TestDataset example = Retail.INSTANCE;


  @BeforeEach
  public void setup() {
    initialize(IntegrationTestSettings.builder()
            .stream(IntegrationTestSettings.StreamEngine.FLINK)
            .database(IntegrationTestSettings.DatabaseEngine.POSTGRES).build(),
        (Path) null, Optional.empty());
  }

  @SneakyThrows
  @Test
  @Disabled
  public void testFlinkTableAPIIntegration() {
    ModuleLoader moduleLoader = injector.getInstance(ModuleLoader.class);

    TableSource tblSource = loadTable(NamePath.of("ecommerce-data", "Orders"), moduleLoader);

    AbstractFlinkStreamEngine flink = new FlinkEngineFactory().initialize(SqrlConfig.EMPTY);
    FlinkStreamBuilder streamBuilder = null;//flink.createJob();
    StreamInputPreparer streamPreparer = new StreamInputPreparerImpl();


    StreamHolder<SourceRecord.Raw> stream = streamPreparer.getRawInput(tblSource, streamBuilder, ErrorPrefix.INPUT_DATA);
    FlexibleSchemaValidator schemaValidator = (FlexibleSchemaValidator) tblSource.getSchema().getValidator(
            tblSource.getConfiguration().getSchemaAdjustmentSettings(),
            tblSource.getConnectorSettings());
    FlinkStreamHolder<SourceRecord.Named> flinkStream = (FlinkStreamHolder)stream.mapWithError(
//        (schemaValidator.getFunction(),//todo come back to
        null,
        ErrorPrefix.INPUT_DATA, SourceRecord.Named.class);

    //TODO: error handling when mapping doesn't work?
    RelDataType tableType = SchemaToRelDataTypeFactory.load(tblSource.getSchema())
        .map(tblSource.getSchema(), tblSource.getName(),
            ErrorCollector.root());
    UniversalTable table = null;
    Schema flinkSchema = new UniversalTable2FlinkSchema().convertSchema(table);
    TypeInformation typeInformation = new FlinkTypeInfoSchemaGenerator()
        .convertSchema(table);
    RowMapper rowMapper = tblSource.getSchema().getRowMapper(FlinkRowConstructor.INSTANCE, tblSource.getConnectorSettings());
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

  protected TableSource loadTable(NamePath path, ModuleLoader moduleLoader) {
    TableSourceNamespaceObject ns = (TableSourceNamespaceObject)moduleLoader
        .getModule(path.popLast())
        .get()
        .getNamespaceObject(path.getLast())
        .get();
    return ns.getTable();
  }
}
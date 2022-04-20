package ai.datasqrl.flink;

import ai.datasqrl.C360Example;
import ai.datasqrl.api.ConfigurationTest;
import ai.datasqrl.Environment;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.execute.flink.environment.FlinkStreamEngine;
import ai.datasqrl.execute.flink.environment.LocalFlinkStreamEngineImpl;
import ai.datasqrl.execute.flink.ingest.DataStreamProvider;
import ai.datasqrl.execute.flink.ingest.SchemaValidationProcess;
import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.server.ImportManager;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.schema.type.schema.SchemaAdjustmentSettings;
import java.nio.file.Path;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.table.api.Expressions.call;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IngestAndSchemaTest {

  Environment env = null;
  DatasetRegistry registry = null;

  @BeforeEach
  public void setup() throws IOException {
      FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
      SqrlSettings settings = ConfigurationTest.getDefaultSettings(true);
      env = Environment.create(settings);
      registry = env.getDatasetRegistry();
  }

  @AfterEach
  public void close() {
      env.close();
      env = null;
      registry = null;
  }

  @SneakyThrows
  @Test
  public void testDatasetMonitoring() {
    ErrorCollector errors = ErrorCollector.root();

    String dsName = "bookclub";
    DirectorySourceImplementation fileConfig = DirectorySourceImplementation.builder()
            .uri(ConfigurationTest.DATA_DIR.toAbsolutePath().toString())
            .build();
    registry.addOrUpdateSource(dsName, fileConfig, errors);
    System.out.println(errors);
    assertFalse(errors.isFatal());

    String ds2Name = "c360";
    fileConfig = DirectorySourceImplementation.builder()
            .uri(C360Example.RETAIL_DATA_DIR.toAbsolutePath().toString())
            .build();
    registry.addOrUpdateSource(ds2Name, fileConfig, errors);
    assertFalse(errors.isFatal());


    //Needs some time to wait for the flink pipeline to compile data
    Thread.sleep(2000);

    SourceDataset ds = registry.getDataset(Name.system(dsName));
    SourceTable book = ds.getTable("book");
    SourceTable person = ds.getTable("person");

    SourceTableStatistics stats = book.getStatistics();
    assertNotNull(stats);
    assertEquals(4,stats.getCount());
    assertEquals(5, person.getStatistics().getCount());

    ImportManager imports = new ImportManager(registry);
    FlinkTableConverter tbConverter = new FlinkTableConverter();

    ErrorCollector schemaErrs = ErrorCollector.root();
    ImportManager.SourceTableImport bookImp = imports.importTable(Name.system(dsName),Name.system("book"),schemaErrs);
    Pair<Schema, TypeInformation> bookSchema = tbConverter.tableSchemaConversion(bookImp.getSourceSchema());
    ImportManager.SourceTableImport ordersImp = imports.importTable(Name.system(ds2Name),Name.system("orders"),schemaErrs);
    Pair<Schema, TypeInformation> ordersSchema = tbConverter.tableSchemaConversion(ordersImp.getSourceSchema());

    LocalFlinkStreamEngineImpl flink = new LocalFlinkStreamEngineImpl();
    FlinkStreamEngine.Builder streamBuilder = flink.createStream();
    StreamExecutionEnvironment env = streamBuilder.getEnvironment();
    StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl)StreamTableEnvironment.create(env);
    final OutputTag<SchemaValidationProcess.Error> schemaErrorTag = new OutputTag<>("SCHEMA_ERROR"){};

    ImportManager.SourceTableImport imp = ordersImp;
    Pair<Schema, TypeInformation> schema = ordersSchema;

    DataStream<SourceRecord.Raw> stream = new DataStreamProvider().getDataStream(imp.getTable(),streamBuilder);
    SingleOutputStreamOperator<SourceRecord.Named> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, imp.getSourceSchema(),
            SchemaAdjustmentSettings.DEFAULT, imp.getTable().getDataset().getDigest()));
    SingleOutputStreamOperator<Row> rows = validate.map(tbConverter.getRowMapper(imp.getSourceSchema()),schema.getRight());

    Table table = tEnv.fromDataStream(rows,schema.getLeft());

    tEnv.createTemporaryView("TheTable", table);
    table.printSchema();

//        Table sum = table.select($("id").sum().as("sum"));
//        tEnv.toChangelogStream(sum).print();

    Table tableShredding = tEnv.sqlQuery("SELECT  o._uuid, items._idx, o.customerid, items.discount, items.quantity, items.productid, items.unit_price \n" +
            "FROM TheTable o CROSS JOIN UNNEST(o.entries) AS items");

    RelNode node = ((PlannerQueryOperation)((TableImpl)tableShredding).getQueryOperation()).getCalciteTree();
    System.out.println(node.explain());
    PlannerQueryOperation plannerQueryOperation = new PlannerQueryOperation(
        FlinkRelBuilder.of(node.getCluster(), null)
        .push(node)
        .project(RexInputRef.of(3, node.getRowType()))
        .build()
    );

//        Table logicalPlanTable = CreateOperation.create(tEnv, plannerQueryOperation);

    List<Operation> create = tEnv.getParser().parse("CREATE TABLE MyUserTable (\n"
        + "  discount DOUBLE"
        + ") WITH (\n"
        + "   'connector' = 'jdbc',\n"
        + "   'url' = 'jdbc:mysql://localhost:3306/mydatabase',\n"
        + "   'table-name' = 'users'\n"
        + ")\n");

    System.out.println(create);

    tEnv.executeSql("CREATE TABLE MyUserTable (\n"
        + "  discount DOUBLE\n"
        + ") WITH (\n"
        + "   'connector' = 'jdbc',\n"
        + "   'url' = 'jdbc:postgresql://localhost/henneberger',\n"
        + "   'table-name' = 'users'\n"
        + ")\n");

    System.out.println( "Insert " +tEnv.getParser().parse("INSERT INTO MyUserTable\n"
        + "SELECT discount FROM "+tableShredding+""));

    StatementSet stmtSet = tEnv.createStatementSet();
    stmtSet.addInsertSql("INSERT INTO MyUserTable\n"
        + "SELECT discount FROM "+tableShredding+"");

    System.out.println(stmtSet.explain(ExplainDetail.JSON_EXECUTION_PLAN));

    TableResult tableResult2 = stmtSet.execute();

//        Table flattenEntries = table.joinLateral(call("UNNEST", $("entries")
//                ))
//                .select($("id"),$("productid"),$("quantity"),$("_idx"));
    tEnv.toChangelogStream(tableShredding).print();

    env.execute();

  }

}

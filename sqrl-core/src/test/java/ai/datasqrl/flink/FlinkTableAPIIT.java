package ai.datasqrl.flink;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.util.StreamInputPreparer;
import ai.datasqrl.io.sources.util.StreamInputPreparerImpl;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.stream.StreamHolder;
import ai.datasqrl.physical.stream.flink.FlinkStreamEngine;
import ai.datasqrl.physical.stream.flink.LocalFlinkStreamEngineImpl;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.schema.input.SchemaValidator;
import ai.datasqrl.util.data.C360;
import lombok.SneakyThrows;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FlinkTableAPIIT extends AbstractSQRLIT {

  @BeforeEach
  public void setup() {
      initialize(IntegrationTestSettings.getFlinkWithDB());
  }

  @SneakyThrows
  @Test
  public void testFlinkTableAPIIntegration() {
    C360 example = C360.INSTANCE;
    example.registerSource(env);

    ImportManager imports = new ImportManager(sourceRegistry);

    ErrorCollector schemaErrs = ErrorCollector.root();
    ImportManager.SourceTableImport ordersImp = (ImportManager.SourceTableImport) imports.importTable(Name.system(example.getName()),Name.system("orders"),
            SchemaAdjustmentSettings.DEFAULT,schemaErrs);
    ImportManager.SourceTableImport imp = ordersImp;

    LocalFlinkStreamEngineImpl flink = new LocalFlinkStreamEngineImpl();
    FlinkStreamEngine.Builder streamBuilder = flink.createJob();
    StreamInputPreparer streamPreparer = new StreamInputPreparerImpl();

    StreamHolder<SourceRecord.Raw> stream = streamPreparer.getRawInput(imp.getTable(),streamBuilder);
    SchemaValidator schemaValidator = new SchemaValidator(imp.getSchema(), SchemaAdjustmentSettings.DEFAULT, imp.getTable().getDataset().getDigest());
    StreamHolder<SourceRecord.Named> validate = stream.mapWithError(schemaValidator.getFunction(),"schema", SourceRecord.Named.class);
    streamBuilder.addAsTable(validate, imp.getSchema(), "thetable");

    StreamTableEnvironment tEnv = streamBuilder.getTableEnvironment();

    Table tableShredding = tEnv.sqlQuery("SELECT  o._uuid, items._idx, o.customerid, items.discount, items.quantity, items.productid, items.unit_price \n" +
            "FROM thetable o CROSS JOIN UNNEST(o.entries) AS items");

    tEnv.toChangelogStream(tableShredding).print();
    streamBuilder.setJobType(FlinkStreamEngine.JobType.SCRIPT);
    streamBuilder.build().execute("test");
  }
}
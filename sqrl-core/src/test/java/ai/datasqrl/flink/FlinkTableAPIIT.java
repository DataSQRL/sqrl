package ai.datasqrl.flink;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.io.sources.util.StreamInputPreparer;
import ai.datasqrl.io.sources.util.StreamInputPreparerImpl;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.physical.stream.StreamHolder;
import ai.datasqrl.physical.stream.flink.FlinkStreamEngine;
import ai.datasqrl.physical.stream.flink.LocalFlinkStreamEngineImpl;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.schema.input.SchemaValidator;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.Retail;
import lombok.SneakyThrows;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class FlinkTableAPIIT extends AbstractSQRLIT {

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

    TableSource tblSource = loadTable(NamePath.of("ecommerce-data","Orders"));

    LocalFlinkStreamEngineImpl flink = new LocalFlinkStreamEngineImpl();
    FlinkStreamEngine.Builder streamBuilder = flink.createJob();
    StreamInputPreparer streamPreparer = new StreamInputPreparerImpl();

    StreamHolder<SourceRecord.Raw> stream = streamPreparer.getRawInput(tblSource,streamBuilder);
    SchemaValidator schemaValidator = new SchemaValidator(tblSource.getSchema(), SchemaAdjustmentSettings.DEFAULT, tblSource.getDigest());
    StreamHolder<SourceRecord.Named> validate = stream.mapWithError(schemaValidator.getFunction(),"schema", SourceRecord.Named.class);
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
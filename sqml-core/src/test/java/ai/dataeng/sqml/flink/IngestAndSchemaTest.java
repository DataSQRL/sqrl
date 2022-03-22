package ai.dataeng.sqml.flink;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.api.ConfigurationTest;
import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.execution.flink.ingest.DataStreamProvider;
import ai.dataeng.sqml.execution.flink.ingest.SchemaValidationProcess;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlinkTableConverter;
import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.io.sources.stats.SourceTableStatistics;
import ai.dataeng.sqml.planner.operator.ImportManager;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.schema.SchemaAdjustmentSettings;
import ai.dataeng.sqml.type.schema.SchemaConversionError;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.table.api.Expressions.$;
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
        String dsName = "bookclub";

        FileSourceConfiguration fileConfig = FileSourceConfiguration.builder()
                .uri(ConfigurationTest.DATA_DIR.toAbsolutePath().toString())
                .name(dsName)
                .build();

        ProcessMessage.ProcessBundle<ConfigurationError> errors = new ProcessMessage.ProcessBundle<>();
        registry.addOrUpdateSource(fileConfig, errors);
        assertFalse(errors.isFatal());

        //Needs some time to wait for the flink pipeline to compile data
        Thread.sleep(2000);

        SourceDataset ds = registry.getDataset(dsName);
        SourceTable book = ds.getTable("book");
        SourceTable person = ds.getTable("person");

        SourceTableStatistics stats = book.getStatistics();
        assertNotNull(stats);
        assertEquals(4,stats.getCount());
        assertEquals(5, person.getStatistics().getCount());

        ImportManager imports = new ImportManager(registry);
        ProcessMessage.ProcessBundle<SchemaConversionError> schemaErrs = new ProcessMessage.ProcessBundle<>();
        ImportManager.SourceTableImport bookImp = imports.importTable(Name.system(dsName),Name.system("book"),schemaErrs);


        FlinkTableConverter tbConverter = new FlinkTableConverter();
        Pair<Schema, TypeInformation> bookSchema = tbConverter.tableSchemaConversion(bookImp.getSourceSchema());


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        final OutputTag<SchemaValidationProcess.Error> schemaErrorTag = new OutputTag<>("SCHEMA_ERROR"){};
        DataStream<SourceRecord.Raw> stream = new DataStreamProvider().getDataStream(bookImp.getTable(),env);
        SingleOutputStreamOperator<SourceRecord.Named> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, bookImp.getSourceSchema(),
                SchemaAdjustmentSettings.DEFAULT, bookImp.getTable().getDataset().getDigest()));

        SingleOutputStreamOperator<Row> rows = validate.map(tbConverter.getRowMapper(bookImp.getSourceSchema()),bookSchema.getRight());

        Table table = tEnv.fromDataStream(rows,bookSchema.getLeft());
        table.printSchema();

        Table select = table.select($("id").sum().as("sum"));

        tEnv.toChangelogStream(select).print();
        env.execute();

    }

}

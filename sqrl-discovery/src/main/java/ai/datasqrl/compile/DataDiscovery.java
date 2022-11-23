package ai.datasqrl.compile;

import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.metadata.MetadataNamedPersistence;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.TableStatisticsStoreProvider;
import ai.datasqrl.io.sources.DataSystem;
import ai.datasqrl.io.sources.DataSystemConfig;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.TableInput;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.io.sources.stats.SchemaGenerator;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.io.sources.stats.TableStatisticsStore;
import ai.datasqrl.io.sources.util.StreamInputPreparer;
import ai.datasqrl.io.sources.util.StreamInputPreparerImpl;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.StreamHolder;
import ai.datasqrl.physical.stream.flink.LocalFlinkStreamEngineImpl;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataDiscovery {

    private final SqrlSettings settings;
    private final StreamEngine streamEngine;
    private final TableStatisticsStoreProvider.Encapsulated statsStore;
    private final StreamInputPreparer streamPreparer;

    public DataDiscovery(SqrlSettings settings) {
        this.settings = settings;
        DatabaseConnectionProvider dbConnection = settings.getDatabaseEngineProvider().getDatabase(
                settings.getDiscoveryConfiguration().getMetastore().getDatabaseName());
//        streamEngine = settings.getStreamEngineProvider().create();
        streamEngine = new LocalFlinkStreamEngineImpl();
        statsStore = new TableStatisticsStoreProvider.EncapsulatedImpl(
                dbConnection, settings.getMetadataStoreProvider(), settings.getSerializerProvider(),
            new MetadataNamedPersistence.TableStatsProvider());
        streamPreparer = new StreamInputPreparerImpl();
    }

    public List<TableInput> discoverTables(DataSystemConfig discoveryConfig, ErrorCollector errors) {
        DataSystem dataSystem = discoveryConfig.initialize(errors);
        if (dataSystem==null) return List.of();

        NamePath path = NamePath.of(dataSystem.getName());
        List<TableInput> tables = dataSystem.getDatasource().discoverSources(dataSystem.getConfig(),errors)
                .stream().map(tblConfig -> tblConfig.initializeInput(errors,path))
                .filter(tbl -> tbl!=null && streamPreparer.isRawInput(tbl))
                .collect(Collectors.toList());
        return tables;
    }

    public void monitorTables(List<TableInput> tables, ErrorCollector errors) {
        try (TableStatisticsStore store = statsStore.openStore()) {
        } catch (IOException e) {
            errors.fatal("Could not open statistics store");

        }
        StreamEngine.Builder streamBuilder = streamEngine.createJob();
        for (TableInput table : tables) {
            StreamHolder<SourceRecord.Raw> stream = streamPreparer.getRawInput(table,streamBuilder);
            stream = streamBuilder.monitor(stream, table, statsStore);
            stream.printSink();
        }
        StreamEngine.Job job = streamBuilder.build();
        job.execute("monitoring["+tables.size()+"]"+tables.hashCode());
    }


    public List<TableSource> discoverSchema(List<TableInput> tables, ErrorCollector errors) {
        return discoverSchema(tables, FlexibleDatasetSchema.EMPTY, errors);
    }

    public List<TableSource> discoverSchema(List<TableInput> tables, FlexibleDatasetSchema baseSchema, ErrorCollector errors) {
        List<TableSource> resultTables = new ArrayList<>();
        try (TableStatisticsStore store = statsStore.openStore()) {
            for (TableInput table : tables) {
                SourceTableStatistics stats = store.getTableStatistics(table.getPath());
                SchemaGenerator generator = new SchemaGenerator(SchemaAdjustmentSettings.DEFAULT);
                FlexibleDatasetSchema.TableField tableField = baseSchema.getFieldByName(table.getName());
                FlexibleDatasetSchema.TableField schema;
                ErrorCollector subErrors = errors.resolve(table.getName());
                if (tableField==null) {
                    schema = generator.mergeSchema(stats, table.getName(), subErrors);
                } else {
                    schema = generator.mergeSchema(stats, tableField, subErrors);
                }
                TableSource tblSource = table.getConfiguration().initializeSource(errors,table.getPath().parent(),schema);
                resultTables.add(tblSource);
            }
        } catch (IOException e) {
            errors.fatal("Could not read statistics from store");

        }
        return resultTables;
    }

    public static FlexibleDatasetSchema combineSchema(List<TableSource> tables) {
        return combineSchema(tables,FlexibleDatasetSchema.EMPTY);
    }

    public static FlexibleDatasetSchema combineSchema(List<TableSource> tables, FlexibleDatasetSchema baseSchema) {
        FlexibleDatasetSchema.Builder builder = new FlexibleDatasetSchema.Builder();
        builder.setDescription(baseSchema.getDescription());
        for (TableSource table : tables) {
            builder.add(table.getSchema().getSchema());
        }
        return builder.build();
    }



}

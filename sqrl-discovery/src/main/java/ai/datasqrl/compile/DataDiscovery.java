package ai.datasqrl.compile;

import ai.datasqrl.config.EngineSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.DataSystem;
import ai.datasqrl.io.sources.DataSystemConfig;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.TableInput;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.io.sources.stats.SchemaGenerator;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.io.sources.stats.TableStatisticsStore;
import ai.datasqrl.io.sources.stats.TableStatisticsStoreProvider;
import ai.datasqrl.io.sources.util.StreamInputPreparer;
import ai.datasqrl.io.sources.util.StreamInputPreparerImpl;
import ai.datasqrl.metadata.MetadataNamedPersistence;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.StreamHolder;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataDiscovery {

    private final ErrorCollector errors;
    private final EngineSettings settings;
    private final StreamEngine streamEngine;
    private final TableStatisticsStoreProvider.Encapsulated statsStore;
    private final StreamInputPreparer streamPreparer;

    public DataDiscovery(ErrorCollector errors, EngineSettings settings) {
        this.errors = errors;
        this.settings = settings;
        streamEngine = settings.getStream(errors);
        statsStore = new TableStatisticsStoreProvider.EncapsulatedImpl(
                settings.getMetadataStoreProvider(),
            new MetadataNamedPersistence.TableStatsProvider());
        streamPreparer = new StreamInputPreparerImpl();
    }

    public List<TableInput> discoverTables(DataSystemConfig discoveryConfig) {
        DataSystem dataSystem = discoveryConfig.initialize(errors);
        if (dataSystem==null) return List.of();

        NamePath path = NamePath.of(dataSystem.getName());
        List<TableInput> tables = dataSystem.getDatasource().discoverSources(dataSystem.getConfig(),errors)
                .stream().map(tblConfig -> tblConfig.initializeInput(errors,path))
                .filter(tbl -> tbl!=null && streamPreparer.isRawInput(tbl))
                .collect(Collectors.toList());
        return tables;
    }

    public void monitorTables(List<TableInput> tables) {
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


    public List<TableSource> discoverSchema(List<TableInput> tables) {
        return discoverSchema(tables, FlexibleDatasetSchema.EMPTY);
    }

    public List<TableSource> discoverSchema(List<TableInput> tables, FlexibleDatasetSchema baseSchema) {
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

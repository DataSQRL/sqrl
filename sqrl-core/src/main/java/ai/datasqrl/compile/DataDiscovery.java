package ai.datasqrl.compile;

import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.TableStatisticsStoreProvider;
import ai.datasqrl.io.sources.DataSystem;
import ai.datasqrl.io.sources.DataSystemConfig;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.TableInput;
import ai.datasqrl.io.sources.stats.SchemaGenerator;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.io.sources.stats.TableStatisticsStore;
import ai.datasqrl.io.sources.util.StreamInputPreparer;
import ai.datasqrl.io.sources.util.StreamInputPreparerImpl;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.StreamHolder;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        streamEngine = settings.getStreamEngineProvider().create();
        statsStore = new TableStatisticsStoreProvider.EncapsulatedImpl(
                dbConnection, settings.getMetadataStoreProvider(), settings.getSerializerProvider(),
                settings.getTableStatisticsStoreProvider());
        streamPreparer = new StreamInputPreparerImpl();
    }

    public Optional<NamePath> monitorTables(DataSystemConfig discoveryConfig, ErrorCollector errors) {
        DataSystem dataSystem = discoveryConfig.initialize(errors);
        if (dataSystem==null) return Optional.empty();

        NamePath path = NamePath.of(dataSystem.getName());
        List<TableInput> tables = dataSystem.getDatasource().discoverTables(dataSystem.getConfig(),errors)
                .stream().map(tblConfig -> tblConfig.initializeInput(errors,path))
                .filter(tbl -> tbl!=null && streamPreparer.isRawInput(tbl))
                .collect(Collectors.toList());
        if (tables.isEmpty()) return Optional.empty();

        StreamEngine.Builder streamBuilder = streamEngine.createJob();
        for (TableInput table : tables) {
            StreamHolder<SourceRecord.Raw> stream = streamPreparer.getRawInput(table,streamBuilder);
            stream = streamBuilder.monitor(stream, table, statsStore);
            stream.printSink();
        }
        StreamEngine.Job job = streamBuilder.build();
        job.execute(path.getDisplay()+"-monitor");
        return Optional.of(path);
    }

    public FlexibleDatasetSchema discoverSchema(NamePath datasetPath, ErrorCollector errors) {
        return discoverSchema(datasetPath, FlexibleDatasetSchema.EMPTY, errors);
    }

    public FlexibleDatasetSchema discoverSchema(NamePath datasetPath, FlexibleDatasetSchema baseSchema, ErrorCollector errors) {
        Map<Name,SourceTableStatistics> tableStats;
        try (TableStatisticsStore store = statsStore.openStore()) {
            tableStats = store.getTablesStatistics(datasetPath);
        } catch (IOException e) {
            errors.fatal("Could not read statistics from store");
            return null;
        }

        FlexibleDatasetSchema.Builder builder = new FlexibleDatasetSchema.Builder();
        builder.setDescription(baseSchema.getDescription());
        for (Map.Entry<Name, SourceTableStatistics> entry : tableStats.entrySet()) {
            SchemaGenerator generator = new SchemaGenerator(SchemaAdjustmentSettings.DEFAULT);
            FlexibleDatasetSchema.TableField tableField = baseSchema.getFieldByName(entry.getKey());
            FlexibleDatasetSchema.TableField result;
            ErrorCollector subErrors = errors.resolve(entry.getKey());
            if (tableField==null) {
                result = generator.mergeSchema(entry.getValue(),entry.getKey(),subErrors);
            } else {
                result = generator.mergeSchema(entry.getValue(),tableField, subErrors);
            }
            builder.add(result);
        }
        return builder.build();
    }



}

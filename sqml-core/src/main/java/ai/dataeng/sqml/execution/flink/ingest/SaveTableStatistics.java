package ai.dataeng.sqml.execution.flink.ingest;

import ai.dataeng.sqml.config.metadata.MetadataStore;
import ai.dataeng.sqml.config.provider.DatasetRegistryPersistenceProvider;
import ai.dataeng.sqml.config.provider.JDBCConnectionProvider;
import ai.dataeng.sqml.config.provider.MetadataStoreProvider;
import ai.dataeng.sqml.io.sources.stats.SourceTableStatistics;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistryPersistence;
import ai.dataeng.sqml.tree.name.Name;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


public class SaveTableStatistics extends RichSinkFunction<SourceTableStatistics> {

    private JDBCConnectionProvider jdbc;
    private MetadataStoreProvider metaProvider;
    private DatasetRegistryPersistenceProvider registryProvider;
    private Name dataset;
    private Name table;

    private transient MetadataStore store;
    private transient DatasetRegistryPersistence persistence;

    public SaveTableStatistics(JDBCConnectionProvider jdbc, MetadataStoreProvider metaProvider,
                               DatasetRegistryPersistenceProvider registryProvider, Name dataset, Name table) {
        this.jdbc = jdbc;
        this.metaProvider = metaProvider;
        this.registryProvider = registryProvider;
        this.dataset = dataset;
        this.table = table;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        store = metaProvider.openStore(jdbc);
        persistence = registryProvider.createRegistryPersistence(store);
    }

    @Override
    public void close() throws Exception {
        store.close();
        store = null;
        persistence = null;
    }

    @Override
    public void invoke(SourceTableStatistics stats, Context context) throws Exception {
        persistence.putTableStatistics(dataset,table,stats);
    }

}

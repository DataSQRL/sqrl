package ai.datasqrl.execute.flink.ingest;

import ai.datasqrl.config.metadata.MetadataStore;
import ai.datasqrl.config.provider.DatasetRegistryPersistenceProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.provider.MetadataStoreProvider;
import ai.datasqrl.config.provider.SerializerProvider;
import ai.datasqrl.io.sources.dataset.DatasetRegistryPersistence;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


public class SaveTableStatistics extends RichSinkFunction<SourceTableStatistics> {

    private JDBCConnectionProvider jdbc;
    private MetadataStoreProvider metaProvider;
    private SerializerProvider serializer;
    private DatasetRegistryPersistenceProvider registryProvider;
    private Name dataset;
    private Name table;

    private transient MetadataStore store;
    private transient DatasetRegistryPersistence persistence;

    public SaveTableStatistics(JDBCConnectionProvider jdbc, MetadataStoreProvider metaProvider, SerializerProvider serializer,
                               DatasetRegistryPersistenceProvider registryProvider, Name dataset, Name table) {
        this.jdbc = jdbc;
        this.metaProvider = metaProvider;
        this.serializer = serializer;
        this.registryProvider = registryProvider;
        this.dataset = dataset;
        this.table = table;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        store = metaProvider.openStore(jdbc, serializer);
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

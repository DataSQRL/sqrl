package ai.dataeng.sqml.execution.flink.ingest.source;

import ai.dataeng.sqml.catalog.persistence.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.config.provider.DatasetRegistryPersistenceProvider;
import ai.dataeng.sqml.config.provider.HierarchicalKeyValueStoreProvider;
import ai.dataeng.sqml.execution.flink.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistryPersistence;
import ai.dataeng.sqml.tree.name.Name;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


public class SaveTableStatistics extends RichSinkFunction<SourceTableStatistics> {

    private HierarchicalKeyValueStoreProvider hkvsProvider;
    private DatasetRegistryPersistenceProvider persistenceProvider;
    private Name dataset;
    private Name table;

    private transient DatasetRegistryPersistence persistence;

    public SaveTableStatistics(HierarchicalKeyValueStoreProvider hkvsProvider, DatasetRegistryPersistenceProvider persistenceProvider, Name dataset, Name table) {
        this.hkvsProvider = hkvsProvider;
        this.persistenceProvider = persistenceProvider;
        this.dataset = dataset;
        this.table = table;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        persistence = persistenceProvider.createRegistryPersistence(hkvsProvider);
    }

    @Override
    public void close() throws Exception {
        persistence.close();
        persistence = null;
    }

    @Override
    public void invoke(SourceTableStatistics stats, Context context) throws Exception {
        persistence.putTableStatistics(dataset,table,stats);
    }

}

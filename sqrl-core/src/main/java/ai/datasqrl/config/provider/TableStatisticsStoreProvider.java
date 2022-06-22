package ai.datasqrl.config.provider;

import ai.datasqrl.config.metadata.MetadataStore;
import ai.datasqrl.io.sources.dataset.TableStatisticsStore;
import lombok.AllArgsConstructor;

import java.io.Serializable;

public interface TableStatisticsStoreProvider extends Serializable {

    TableStatisticsStore openStore(MetadataStore metaStore);

    interface Encapsulated extends Serializable {

        TableStatisticsStore openStore();

    }

    @AllArgsConstructor
    class EncapsulatedImpl implements Encapsulated {

        private final DatabaseConnectionProvider dbConnection;
        private final MetadataStoreProvider metaProvider;
        private final SerializerProvider serializer;
        private final TableStatisticsStoreProvider statsProvider;

        @Override
        public TableStatisticsStore openStore() {
            MetadataStore store = metaProvider.openStore(dbConnection, serializer);
            return statsProvider.openStore(store);
        }
    }

}

package ai.datasqrl.physical.database.inmemory;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.metadata.MetadataStore;
import ai.datasqrl.metadata.MetadataStoreProvider;
import ai.datasqrl.physical.database.DatabaseEngine;
import ai.datasqrl.physical.database.DatabaseEngineConfiguration;
import lombok.NonNull;

public class InMemoryDatabaseConfiguration implements DatabaseEngineConfiguration {

    public static final String ENGINE_NAME = "hashmap";

    @Override
    public String getEngineName() {
        return ENGINE_NAME;
    }

    @Override
    public DatabaseEngine initialize(@NonNull ErrorCollector errors) {
        return new InMemoryDatabase();
    }

    @Override
    public MetadataStoreProvider getMetadataStore() {
        return new StoreProvider();
    }

    public static class StoreProvider implements MetadataStoreProvider {

        @Override
        public MetadataStore openStore() {
            return new InMemoryMetadataStore();
        }
    }

}

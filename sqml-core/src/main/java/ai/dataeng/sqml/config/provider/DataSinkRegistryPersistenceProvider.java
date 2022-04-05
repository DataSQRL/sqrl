package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.config.metadata.MetadataStore;
import ai.dataeng.sqml.io.sinks.registry.DataSinkRegistryPersistence;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistryPersistence;

import java.io.Serializable;

public interface DataSinkRegistryPersistenceProvider extends Serializable {

    DataSinkRegistryPersistence createRegistryPersistence(MetadataStore metaStore);

}

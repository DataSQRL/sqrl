package ai.datasqrl.config.provider;

import ai.datasqrl.config.metadata.MetadataStore;
import ai.datasqrl.io.sinks.registry.DataSinkRegistryPersistence;
import java.io.Serializable;

public interface DataSinkRegistryPersistenceProvider extends Serializable {

  DataSinkRegistryPersistence createRegistryPersistence(MetadataStore metaStore);

}

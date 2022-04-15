package ai.datasqrl.config.provider;

import ai.datasqrl.config.metadata.MetadataStore;
import ai.datasqrl.io.sources.dataset.DatasetRegistryPersistence;
import java.io.Serializable;

public interface DatasetRegistryPersistenceProvider extends Serializable {

    DatasetRegistryPersistence createRegistryPersistence(MetadataStore metaStore);

}

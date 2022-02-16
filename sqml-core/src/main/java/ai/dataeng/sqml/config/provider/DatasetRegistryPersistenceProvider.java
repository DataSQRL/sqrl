package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.io.sources.dataset.DatasetRegistryPersistence;

import java.io.Serializable;

public interface DatasetRegistryPersistenceProvider  extends Serializable {

    DatasetRegistryPersistence createRegistryPersistence(HierarchicalKeyValueStoreProvider hkvsProvider);

}

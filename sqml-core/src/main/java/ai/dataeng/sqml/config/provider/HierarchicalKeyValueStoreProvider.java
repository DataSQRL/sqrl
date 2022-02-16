package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.catalog.persistence.keyvalue.HierarchyKeyValueStore;

import java.io.Serializable;

public interface HierarchicalKeyValueStoreProvider extends Serializable {

    HierarchyKeyValueStore openStore();

}

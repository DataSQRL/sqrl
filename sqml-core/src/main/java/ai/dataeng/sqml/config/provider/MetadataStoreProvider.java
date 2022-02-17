package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.config.metadata.MetadataStore;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;

import java.io.Serializable;

public interface MetadataStoreProvider extends Serializable {

    MetadataStore openStore(JDBCConnectionProvider jdbc);

}

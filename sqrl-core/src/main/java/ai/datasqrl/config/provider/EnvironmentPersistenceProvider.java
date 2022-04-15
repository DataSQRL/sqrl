package ai.datasqrl.config.provider;

import ai.datasqrl.server.EnvironmentPersistence;
import ai.datasqrl.config.metadata.MetadataStore;
import java.io.Serializable;

public interface EnvironmentPersistenceProvider extends Serializable {

    EnvironmentPersistence createEnvironmentPersistence(MetadataStore metaStore);

}

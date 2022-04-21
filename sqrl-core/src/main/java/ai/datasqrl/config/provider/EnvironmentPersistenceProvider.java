package ai.datasqrl.config.provider;

import ai.datasqrl.config.metadata.MetadataStore;
import ai.datasqrl.server.EnvironmentPersistence;
import java.io.Serializable;

public interface EnvironmentPersistenceProvider extends Serializable {

  EnvironmentPersistence createEnvironmentPersistence(MetadataStore metaStore);

}

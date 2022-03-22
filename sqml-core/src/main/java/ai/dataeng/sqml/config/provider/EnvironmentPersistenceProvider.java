package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.EnvironmentPersistence;
import ai.dataeng.sqml.config.metadata.MetadataStore;
import java.io.Serializable;

public interface EnvironmentPersistenceProvider extends Serializable {

    EnvironmentPersistence createEnvironmentPersistence(MetadataStore metaStore);

}

package ai.datasqrl.config.provider;

import ai.datasqrl.config.metadata.MetadataStore;
import java.io.Serializable;

public interface MetadataStoreProvider extends Serializable {

  MetadataStore openStore(JDBCConnectionProvider jdbc, SerializerProvider serializer);

}

package ai.datasqrl.metadata;

import java.io.Serializable;

public interface MetadataStoreProvider extends Serializable {

  MetadataStore openStore();

}

package ai.datasqrl.io.sinks.registry;

import ai.datasqrl.config.metadata.MetadataStore;
import ai.datasqrl.config.provider.DataSinkRegistryPersistenceProvider;
import ai.datasqrl.io.sinks.DataSinkRegistration;
import ai.datasqrl.parse.tree.name.Name;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MetadataSinkRegistryPersistence implements DataSinkRegistryPersistence {

  public static final String STORE_DATASINK_KEY = "datasinks";
  public static final String STORE_SINK_CONFIG_KEY = "sink";

  private final MetadataStore store;

  @Override
  public Collection<DataSinkRegistration> getSinks() {
    return store.getSubKeys(STORE_DATASINK_KEY).stream().map(sinkName -> {
      DataSinkRegistration config = store.get(DataSinkRegistration.class, STORE_DATASINK_KEY,
          sinkName, STORE_SINK_CONFIG_KEY);
      Preconditions.checkArgument(config != null,
          "Persistence of sink configuration failed.");
      return config;
    }).collect(Collectors.toList());
  }

  @Override
  public void putSink(Name sink, DataSinkRegistration sinkRegistration) {
    store.put(sinkRegistration, STORE_DATASINK_KEY, store.name2Key(sink), STORE_SINK_CONFIG_KEY);
  }

  @Override
  public void removeSink(Name sink) {
    store.remove(STORE_DATASINK_KEY, store.name2Key(sink), STORE_SINK_CONFIG_KEY);
  }

  public static class Provider implements DataSinkRegistryPersistenceProvider {

    @Override
    public DataSinkRegistryPersistence createRegistryPersistence(MetadataStore metaStore) {
      return new MetadataSinkRegistryPersistence(metaStore);
    }
  }
}

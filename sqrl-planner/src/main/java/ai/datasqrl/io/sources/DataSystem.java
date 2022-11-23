package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.dataset.TableConfig;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

@AllArgsConstructor
@Getter
public class DataSystem implements Serializable {

  Name name;
  DataSystemDiscovery datasource;
  DataSystemConfig config;

  public NameCanonicalizer getCanonicalizer() {
    return config.getNameCanonicalizer();
  }

  public Collection<TableConfig> discoverTables(ErrorCollector errors) {
    return datasource.discoverSources(config,errors);
  }

  public Optional<TableConfig> discoverSink(@NonNull Name sinkName, @NonNull ErrorCollector errors) {
    return datasource.discoverSink(sinkName, config, errors);
  }

}

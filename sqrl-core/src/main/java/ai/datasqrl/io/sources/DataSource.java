package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.dataset.TableConfig;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collection;

@AllArgsConstructor
@Getter
public class DataSource implements Serializable {

  public enum Type {
    SOURCE, SINK, SOURCE_AND_SINK;

    public boolean isSource() {
      return this==SOURCE || this==SOURCE_AND_SINK;
    }

    public boolean isSink() {
      return this==SOURCE || this==SOURCE_AND_SINK;
    }
  }

  Name name;
  DataSourceDiscovery datasource;
  DataSourceConfig config;

  public NameCanonicalizer getCanonicalizer() {
    return config.getNameCanonicalizer();
  }

  public Collection<TableConfig> discoverTables(ErrorCollector errors) {
    return datasource.discoverTables(config,errors);
  }

}

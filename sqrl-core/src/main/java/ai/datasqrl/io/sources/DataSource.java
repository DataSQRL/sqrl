package ai.datasqrl.io.sources;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@ToString
public class DataSource implements Serializable {

  String name;
  DataSourceImplementation implementation;
  DataSourceConfiguration config;

  @JsonCreator
  public DataSource(@JsonProperty("name") String name,
      @JsonProperty("implementation") DataSourceImplementation implementation,
      @JsonProperty("config") DataSourceConfiguration config) {
    this.name = name;
    this.implementation = implementation;
    this.config = config;
  }

  public DataSource(DataSourceUpdate update) {
    this(update.getName(), update.getSource(), update.getConfig());
  }

  public NameCanonicalizer getCanonicalizer() {
    return config.getNameCanonicalizer();
  }

  public Name getName() {
    return Name.system(name);
  }

  public DataSourceImplementation getImplementation() {
    return implementation;
  }

  public DataSourceConfiguration getConfig() {
    return config;
  }
}

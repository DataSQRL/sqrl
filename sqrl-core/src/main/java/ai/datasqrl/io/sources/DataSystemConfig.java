package ai.datasqrl.io.sources;

import ai.datasqrl.config.constraints.OptionalMinString;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.SharedConfiguration;
import ai.datasqrl.parse.tree.name.Name;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Optional;

@SuperBuilder
@NoArgsConstructor
@Getter
public class DataSystemConfig extends SharedConfiguration implements Serializable {

  @OptionalMinString
  String name;
  @Valid @NonNull @NotNull
  DataSystemDiscoveryConfig datasource;

  public DataSystem initialize(ErrorCollector errors) {
    DataSystemDiscovery source = datasource.initialize(errors);
    if (!rootInitialize(errors,source.requiresFormat())) return null;

    if (Strings.isNullOrEmpty(name)) {
      Optional<String> discoveredName = source.getDefaultName();
      if (discoveredName.isPresent()) {
        name = discoveredName.get();
      } else {
        errors.fatal("Data source needs a valid name");
        return null;
      }
    }
    return new DataSystem(Name.system(name),source,this);
  }

}

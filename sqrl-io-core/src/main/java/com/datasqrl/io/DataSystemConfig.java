package com.datasqrl.io;

import com.datasqrl.util.constraints.OptionalMinString;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.Name;
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
  DataSystemDiscoveryConfig datadiscovery;

  public DataSystem initialize(ErrorCollector errors) {
    if (!rootInitialize(errors)) {
      return null;
    }
    DataSystemDiscovery source = datadiscovery.initialize(errors);
    if (source == null) {
      return null;
    }
    if (source.requiresFormat(getType()) && getFormat() == null) {
      errors.fatal("Need to configure a format");
      return null;
    }

    if (Strings.isNullOrEmpty(name)) {
      Optional<String> discoveredName = source.getDefaultName();
      if (discoveredName.isPresent()) {
        name = discoveredName.get();
      } else {
        errors.fatal("Data source needs a valid name");
        return null;
      }
    }
    return new DataSystem(Name.system(name), source, this);
  }

}

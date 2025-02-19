package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum SystemBuiltInConnectors {

  PRINT_SINK(Name.system("print"), ExternalDataType.sink),
  LOCAL_FILE_SOURCE(Name.system("localfile"), ExternalDataType.source),
  LOGGER(Name.system("logger"), ExternalDataType.sink),
  LOG_ENGINE(Name.system("log"), ExternalDataType.sink);

  final Name name;
  final ExternalDataType type;

  @Override
  public String toString() {
    return name.getCanonical();
  }

  public static Optional<SystemBuiltInConnectors> forExport(Name name) {
    for (SystemBuiltInConnectors connector : values()) {
      if (connector.type.isSink() && connector.name.equals(name)) {
        return Optional.of(connector);
      }
    }
    return Optional.empty();
  }

  public static Optional<SystemBuiltInConnectors> forExport(NamePath path) {
    if (path.size() == 1) {
      return forExport(path.getFirst());
    }
    return Optional.empty();
  }

}

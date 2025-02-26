package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ModuleLoaderStd implements ModuleLoader {
  private final Map<NamePath, SqrlModule> modules;

  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {
    SqrlModule module;
    if ((module = modules.get(namePath)) != null) {
      return Optional.of(module);
    }
    return Optional.empty();
  }
}

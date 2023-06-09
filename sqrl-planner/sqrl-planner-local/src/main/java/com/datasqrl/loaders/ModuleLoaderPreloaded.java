package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class ModuleLoaderPreloaded implements ModuleLoader {

  @Singular Map<NamePath, SqrlModule> modules;

  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {
    SqrlModule module;
    if ((module = modules.get(namePath))!=null) {
      return Optional.of(module);
    }
    return Optional.empty();
  }
}

package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class ModuleLoaderComposite implements ModuleLoader {

  @Singular
  List<ModuleLoader> moduleLoaders;

  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {
    for (ModuleLoader loader : moduleLoaders) {
      Optional<SqrlModule> module = loader.getModule(namePath);
      if (module.isPresent()) return module;
    }
    return Optional.empty();
  }
}

package com.datasqrl.loaders;

import java.util.List;
import java.util.Optional;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class ModuleLoaderComposite implements ModuleLoader {

  @Singular
  List<ModuleLoader> moduleLoaders;

  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {
    for (ModuleLoader loader : moduleLoaders) {
      var module = loader.getModule(namePath);
      if (module.isPresent()) {
        return module;
      }
    }
    return Optional.empty();
  }
}
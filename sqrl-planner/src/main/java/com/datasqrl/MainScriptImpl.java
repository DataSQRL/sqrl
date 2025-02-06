package com.datasqrl;

import java.nio.file.Path;
import java.util.Optional;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.MainScript;
import com.datasqrl.util.FileUtil;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class MainScriptImpl implements MainScript {

  private final PackageJson config;
  private final ResourceResolver resourceResolver;

  @Override
public String getContent() {
    var mainScript = config.getScriptConfig().getMainScript().map(NamePath::of).flatMap(resourceResolver::resolveFile)
        .orElseThrow(() -> new RuntimeException("Could not find main sqrl script file"));
    return FileUtil.readFile(mainScript);
  }

  @Override
public Optional<Path> getPath() {
    return config.getScriptConfig().getMainScript()
        .map(NamePath::of)
        .flatMap(resourceResolver::resolveFile);
  }
}

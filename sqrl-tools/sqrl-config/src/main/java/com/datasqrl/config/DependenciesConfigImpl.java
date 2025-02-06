package com.datasqrl.config;

import java.util.Map;
import java.util.Optional;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DependenciesConfigImpl implements PackageJson.DependenciesConfig {

  public static final String DEPENDENCIES_KEY = "dependencies";
  public static final String PKG_NAME_KEY = "pkgName";
  public static final String VERSION_KEY = "version";
  public static final String VARIANT_KEY = "variant";

  SqrlConfig parentConfig;
  SqrlConfig sqrlConfig;

  @Override
public void addDependency(String key, Dependency dep) {
    sqrlConfig.getSubConfig(key).setProperties(dep);
  }

  @Override
public Optional<Dependency> getDependency(String dependency) {
    if (!sqrlConfig.hasSubConfig(dependency)) {
      //todo Optional
      return Optional.empty();
    }

    var subConfig = sqrlConfig.getSubConfig(dependency);
    return Optional.of(new DependencyImpl(subConfig));
  }

  @Override
public Map<String, DependencyImpl> getDependencies() {
    return parentConfig.asMap(DEPENDENCIES_KEY, DependencyImpl.class).get();
  }

//  public static LinkedHashMap<String, Dependency> fromRootConfig(@NonNull SqrlConfig config) {
//    return config.asMap(DEPENDENCIES_KEY,Dependency.class).get();
//  }

}

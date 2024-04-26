package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

public interface PackageJson {

  List<String> getEnabledEngines();

  void setPipeline(List<String> pipeline);

  TestConfig getTest();

  EnginesConfig getEngines();

  DiscoveryConfig getDiscovery();

  DependenciesConfig getDependencies();

  boolean hasProfileKey();

  List<String> getProfiles();

  void setProfiles(String[] profiles);

  void toFile(Path path, boolean pretty);

  ScriptConfig getScriptConfig();

  Map<String, Object> toMap();

  CompilerConfig getCompilerConfig();

  PackageConfiguration getPackageConfig();

  int getVersion();

  boolean hasScriptKey();

  interface CompilerConfig {

    ExplainConfig getExplain();

    Optional<String> getSnapshotPath();

    void setSnapshotPath(String string);

    boolean isAddArguments();
  }

  interface ExplainConfig {

    boolean isText();

    boolean isExtended();

    boolean isSorted();

    boolean isVisual();
  }

  interface ScriptConfig {

    Optional<String> getMainScript();

    Optional<String> getGraphql();

    void setMainScript(String script);

    void setGraphql(String graphql);
  }

  interface EnginesConfig {

    Optional<EngineConfig> getEngineConfig(String engineId);
  }

  interface EngineConfig {

    String getEngineName();

    @Deprecated(since="Migrate to templates or static objects")
    Map<String, Object> toMap();

    ConnectorsConfig getConnectors();
  }

  @AllArgsConstructor
  @Getter
  static class EmptyEngineConfig implements EngineConfig {
    String engineName;

    @Override
    public Map<String, Object> toMap() {
      return Map.of();
    }

    @Override
    public ConnectorsConfig getConnectors() {
      return null;
    }
  }

  interface TestConfig {
    int getDurationSec();
    int getDelaySec();
  }

  interface DependenciesConfig {

    void addDependency(String key, Dependency dep);

    Optional<Dependency> getDependency(String profile);

    Map<String, Dependency> getDependencies();
  }

  interface DiscoveryConfig {

    DataDiscoveryConfig getDataDiscoveryConfig();

  }

  interface DataDiscoveryConfig {

    TablePattern getTablePattern(String defaultTablePattern);

    ErrorCollector getErrors();
  }
}

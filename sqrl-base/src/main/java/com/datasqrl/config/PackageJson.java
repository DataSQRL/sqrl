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

  EnginesConfig getEngines();

  DiscoveryConfig getDiscovery();

  DependenciesConfig getDependencies();

  void toFile(Path path, boolean pretty);

  ScriptConfig getScriptConfig();

  Map<String, Object> toMap();

  CompilerConfig getCompilerConfig();

  PackageConfiguration getPackageConfig();

  int getVersion();

  boolean hasScriptKey();

  Optional<TestRunnerConfiguration> getTestConfig();

  interface CompilerConfig {

    ExplainConfig getExplain();

    OutputConfig getOutput();

    Optional<String> getSnapshotPath();

    void setSnapshotPath(String string);

    boolean isAddArguments();

    String getLogger();

    boolean isExtendedScalarTypes();
  }

  interface OutputConfig {

    boolean isAddUid();

    String getTableSuffix();
  }

  interface ExplainConfig {

    boolean isText();

    boolean isSql();

    boolean isLogical();

    boolean isPhysical();

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

    EngineConfig getEngineConfigOrErr(String engineId);
  }

  interface EngineConfig {

    String getEngineName();

    @Deprecated(since = "Migrate to templates or static objects")
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

  interface DependenciesConfig {

    void addDependency(String key, Dependency dep);

    Optional<Dependency> getDependency(String profile);

    Map<String, ? extends Dependency> getDependencies();
  }

  interface DiscoveryConfig {

    DataDiscoveryConfig getDataDiscoveryConfig();
  }

  interface DataDiscoveryConfig {

    TablePattern getTablePattern(String defaultTablePattern);

    ErrorCollector getErrors();
  }
}

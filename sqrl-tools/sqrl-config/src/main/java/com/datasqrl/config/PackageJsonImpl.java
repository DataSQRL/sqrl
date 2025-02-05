package com.datasqrl.config;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

public class PackageJsonImpl implements PackageJson {
  public static final String ENGINES_PROPERTY = "engines";
  public static final String DISCOVERY_KEY = "discovery";
  public static final String DEPENDENCIES_KEY = "dependencies";
  public static final String SCRIPT_KEY = "script";
  public static final String COMPILER_KEY = "compiler";
  public static final String PACKAGE_KEY = "package";
  public static final String PIPELINE_KEY = "enabled-engines";
  public static final String CONNECTORS_KEY = "connectors";
  public static final String TEST_RUNNER_KEY = "test-runner";

  private SqrlConfig sqrlConfig;

  public PackageJsonImpl() {
    this(SqrlConfig.createCurrentVersion());
  }

  public PackageJsonImpl(SqrlConfig sqrlConfig) {
	this.sqrlConfig = sqrlConfig;
  }

  @Override
  public List<String> getEnabledEngines() {
    return sqrlConfig.asList(PIPELINE_KEY, String.class).get();
  }

  @Override
  public void setPipeline(List<String> pipeline) {
    sqrlConfig.setProperty(PIPELINE_KEY, pipeline);
  }

  @Override
  public EnginesConfigImpl getEngines() {
    return new EnginesConfigImpl(sqrlConfig.getSubConfig(ENGINES_PROPERTY));
  }

  @Override
  public DiscoveryConfigImpl getDiscovery() {
    return new DiscoveryConfigImpl(sqrlConfig.getSubConfig(DISCOVERY_KEY));
  }

  @Override
  public DependenciesConfigImpl getDependencies() {
    return new DependenciesConfigImpl(sqrlConfig, sqrlConfig.getSubConfig(DEPENDENCIES_KEY));
  }

  @Override
  public void toFile(Path path, boolean pretty) {
    sqrlConfig.toFile(path, pretty);
  }

  @Override
  public ScriptConfig getScriptConfig() {
    return new ScriptConfigImpl(sqrlConfig.getSubConfig(SCRIPT_KEY));
  }

  @Override
  public Map<String, Object> toMap() {
    return sqrlConfig.toMap();
  }

  @Override
  public CompilerConfigImpl getCompilerConfig() {
    return new CompilerConfigImpl(sqrlConfig.getSubConfig(COMPILER_KEY));
  }

  @Override
  public PackageConfigurationImpl getPackageConfig() {
    return sqrlConfig.getSubConfig(PACKAGE_KEY).allAs(PackageConfigurationImpl.class).get();
  }

  @Override
  public int getVersion() {
    return sqrlConfig.getVersion();
  }

  @Override
  public boolean hasScriptKey() {
    //ScriptConfiguration.SCRIPT_KEY
    return false;
  }

  @Override
  public Optional<TestRunnerConfiguration> getTestConfig() {
    if (sqrlConfig.hasSubConfig(TEST_RUNNER_KEY)) {
      return Optional.of(new TestRunnerConfigImpl(sqrlConfig.getSubConfig(TEST_RUNNER_KEY)));
    }
    return Optional.empty();
  }

  @Override
  public String toString() {
    return "PackageJsonImpl{" +
        "sqrlConfig=" + sqrlConfig.toMap() +
        '}';
  }
}

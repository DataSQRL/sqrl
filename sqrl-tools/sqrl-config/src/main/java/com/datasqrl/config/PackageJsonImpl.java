package com.datasqrl.config;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PackageJsonImpl implements PackageJson {
  public static final String ENGINES_PROPERTY = "engines";
  public static final String TEST_KEY = "test";
  public static final String DISCOVERY_KEY = "discovery";
  public static final String PROFILES_KEY = "profiles";
  public static final String DEPENDENCIES_KEY = "dependencies";
  public static final String SCRIPT_KEY = "script";
  public static final String COMPILER_KEY = "compiler";
  public static final String PACKAGE_KEY = "package";
  public static final String PIPELINE_KEY = "pipeline";

  private SqrlConfig sqrlConfig;

  @Override
  public List<String> getPipeline() {
    return sqrlConfig.asList(PIPELINE_KEY, String.class).get();
  }

  @Override
  public void setPipeline(List<String> pipeline) {
    sqrlConfig.setProperty(PIPELINE_KEY, pipeline);
  }

  @Override
  public TestConfig getTest() {
    return new TestConfigImpl(sqrlConfig.getSubConfig(TEST_KEY));
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
  public void setProfiles(String[] profiles) {
    sqrlConfig.setProperty(PROFILES_KEY, profiles);
  }

  @Override
  public DependenciesConfigImpl getDependencies() {
    return new DependenciesConfigImpl(sqrlConfig.getSubConfig(DEPENDENCIES_KEY));
  }

  @Override
  public boolean hasProfileKey() {
    return sqrlConfig.hasKey(PROFILES_KEY);
  }

  @Override
  public List<String> getProfiles() {
    return sqrlConfig.asList(PROFILES_KEY, String.class)
        .get();
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
  public String toString() {
    return "PackageJsonImpl{" +
        "sqrlConfig=" + sqrlConfig.toMap() +
        '}';
  }
}

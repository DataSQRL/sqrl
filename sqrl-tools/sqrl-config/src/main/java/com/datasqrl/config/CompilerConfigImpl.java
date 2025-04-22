package com.datasqrl.config;

import java.util.Optional;

import com.datasqrl.config.PackageJson.OutputConfig;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class CompilerConfigImpl implements PackageJson.CompilerConfig {

  SqrlConfig sqrlConfig;

  @Override
public void setSnapshotPath(String path) {
    sqrlConfig.setProperty("snapshotPath", path);
  }

  @Override
public Optional<String> getSnapshotPath() {
    return sqrlConfig.asString("snapshotPath").getOptional();
  }

  @Override
public boolean isAddArguments() {
    return sqrlConfig.asBool("addArguments").getOptional().orElse(true);
  }

  @Override
public boolean isExtendedScalarTypes() {
    return sqrlConfig.asBool("extendedScalarTypes").getOptional().orElse(true);
  }

  @Override
  public String getLogger() {
    return sqrlConfig.hasKey("logger") ? sqrlConfig.asString("logger").get() : "print";
  }

  @Override
public ExplainConfigImpl getExplain() {
    return new ExplainConfigImpl(sqrlConfig.getSubConfig("explain"));
  }

  @Override
  public OutputConfig getOutput() {
    return OutputConfigImpl.from(sqrlConfig.getSubConfig("output"));
  }
}

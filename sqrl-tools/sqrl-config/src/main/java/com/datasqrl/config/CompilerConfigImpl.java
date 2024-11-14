package com.datasqrl.config;

import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class CompilerConfigImpl implements PackageJson.CompilerConfig {

  SqrlConfig sqrlConfig;

  public void setSnapshotPath(String path) {
    sqrlConfig.setProperty("snapshotPath", path);
  }

  public Optional<String> getSnapshotPath() {
    return sqrlConfig.asString("snapshotPath").getOptional();
  }

  public boolean isAddArguments() {
    return sqrlConfig.asBool("addArguments")
        .getOptional().orElse(true);
  }

  @Override
  public String getLogger() {
    return sqrlConfig.hasKey("logger") ? sqrlConfig.asString("logger").get() : "print";
  }

  @Override
  public Optional<String> getTargetDataPath() {
    return sqrlConfig.asString("target-data-path").getOptional();
  }

  @Override
  public Optional<String> getTargetLibPath() {
    return sqrlConfig.asString("target-lib-path").getOptional();
  }

  public ExplainConfigImpl getExplain() {
    return new ExplainConfigImpl(sqrlConfig.getSubConfig("explain"));
  }

}

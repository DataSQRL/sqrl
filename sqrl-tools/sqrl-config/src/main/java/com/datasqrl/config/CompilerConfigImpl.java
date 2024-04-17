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

  public ExplainConfigImpl getExplain() {
    return new ExplainConfigImpl(sqrlConfig.getSubConfig("explain"));
  }

}

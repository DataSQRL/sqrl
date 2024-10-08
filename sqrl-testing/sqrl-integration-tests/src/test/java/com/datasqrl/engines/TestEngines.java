package com.datasqrl.engines;

import com.datasqrl.config.PackageJson;
import com.datasqrl.engines.TestEngine.TestEngineVisitor;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TestEngines {

  private final PackageJson packageJson;
  private final List<TestEngine> testEngines;

  public <R, C> R accept(TestEngineVisitor<R, C> visitor, C c) {
    return visitor.accept(this, c);
  }
}

package com.datasqrl.injector;

import com.datasqrl.engine.PhysicalPlan;

public interface PostplanHook {
  public void runHook(PhysicalPlan plan);
}

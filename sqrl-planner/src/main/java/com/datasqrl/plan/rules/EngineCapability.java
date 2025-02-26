package com.datasqrl.plan.rules;

import com.datasqrl.engine.EngineFeature;
import lombok.Value;
import org.apache.calcite.sql.SqlOperator;

public interface EngineCapability {

  String getName();

  @Value
  class Feature implements EngineCapability {

    EngineFeature feature;

    @Override
    public String getName() {
      return feature.name() + " (feature)";
    }

    @Override
    public String toString() {
      return getName();
    }
  }

  @Value
  class Function implements EngineCapability {

    SqlOperator function;

    @Override
    public String getName() {
      return function.getName() + " (function)";
    }

    @Override
    public String toString() {
      return getName();
    }
  }
}

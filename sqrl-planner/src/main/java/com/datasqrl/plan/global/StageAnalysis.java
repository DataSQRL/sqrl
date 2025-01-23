package com.datasqrl.plan.global;

import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.rules.ComputeCost;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.plan.rules.ExecutionAnalysis.CapabilityException;
import java.util.Collection;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Getter
@AllArgsConstructor
public abstract class StageAnalysis {

  private final ExecutionStage stage;

  public abstract String getMessage();

  public boolean isSupported() {
    return false;
  }

  public String getName() {
    return stage.getName();
  }

  public static StageAnalysis.MissingCapability of(CapabilityException exception) {
    return new MissingCapability(exception.getStage(), exception.getCapabilities());
  }


  @Value
  @EqualsAndHashCode(callSuper = true)
  public static class Cost extends StageAnalysis {

    ComputeCost cost;
    boolean supported;

    public Cost(ExecutionStage stage, ComputeCost cost, boolean supported) {
      super(stage);
      this.cost = cost;
      this.supported = supported;
    }

    @Override
    public String getMessage() {
      if (supported) {
        return String.format("Stage [%s] supported at cost: %s", getName(), cost);
      } else {
        return String.format("Stage [%s] has been eliminated  due to high cost: %s",getName(), cost);
      }
    }

    @Override
    public boolean isSupported() {
      return supported;
    }

    public Cost tooExpensive() {
      return new Cost(super.getStage(), cost, false);
    }

  };



  @Value
  @EqualsAndHashCode(callSuper = true)
  public static class MissingCapability extends StageAnalysis {

    Collection<EngineCapability> capabilities;

    public MissingCapability(ExecutionStage stage, Collection<EngineCapability> capabilities) {
      super(stage);
      this.capabilities = capabilities;
    }

    @Override
    public String getMessage() {
      return String.format("Stage [%s] does not support capabilities: %s", getName(), capabilities);
    }
  }

  @Value
  @EqualsAndHashCode(callSuper = true)
  public static class MissingDependent extends StageAnalysis {

    boolean upstream; //else it's downstream
    String tableName;

    public MissingDependent(ExecutionStage stage, boolean upstream, String tableName) {
      super(stage);
      this.upstream = upstream;
      this.tableName = tableName;
    }

    @Override
    public String getMessage() {
      return String.format("%s [%s] does not support stage [%s] or any %s stages",
          upstream?"Upstream input":"Downstream consumer",
          tableName, getName(),
          upstream?"prior":"subsequent");
    }
  }

}

package com.datasqrl.plan.calcite.rules;

import com.datasqrl.name.NamePath;
import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.pipeline.ExecutionStage;
import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class ExecutionStageException extends RuntimeException {

  protected List<AnnotatedLP> inputs;

  protected ExecutionStageException(String msg) {
    super(msg);
  }

  public ExecutionStageException injectInput(@NonNull AnnotatedLP input) {
    return injectInputs(List.of(input));
  }

  public ExecutionStageException injectInputs(@NonNull List<AnnotatedLP> inputs) {
    this.inputs = inputs;
    return this;
  }

  public static class StageChange extends ExecutionStageException {

    public StageChange(String msg) {
      super(msg);
    }

    public static StageChange of(ExecutionStage start, ExecutionStage end) {
      return new StageChange(
          String.format("Processing relation requires stage change from [%s] to [%s]"
              , start.getName(), end.getName()));
    }

  }

  public static class StageFinding extends ExecutionStageException {

    public StageFinding(String msg) {
      super(msg);
    }

    public static StageFinding of(ExecutionStage baseStage,
        Collection<EngineCapability> capabilities) {
      return new StageFinding(
          String.format("Could not find stage has capabilities [%s] starting at stage [%s]",
              capabilities, baseStage.getName()));
    }

    public static StageFinding of(ExecutionStage baseStage, ExecutionStage other) {
      return new StageFinding(
          String.format("Could not find stage that combines [%s] starting at [%s]",
              other.getName(), baseStage.getName()));
    }


  }

  public static class StageIncompatibility extends ExecutionStageException {

    public StageIncompatibility(String msg) {
      super(msg);
    }

  }

  @AllArgsConstructor
  public static class NoStage extends RuntimeException {

    final NamePath relationName;
    final Map<ExecutionStage, ExecutionStageException> stageExceptions;

    @Override
    public String getMessage() {
      StringBuilder s = new StringBuilder();
      s.append("Could not find stage that could process table:").append(relationName.getDisplay())
          .append("\n");
      for (Map.Entry<ExecutionStage, ExecutionStageException> entry : stageExceptions.entrySet()) {
        s.append(entry.getKey().getName()).append(" -> ").append(entry.getValue().getMessage())
            .append("\n");
      }
      return s.toString();
    }

  }

}

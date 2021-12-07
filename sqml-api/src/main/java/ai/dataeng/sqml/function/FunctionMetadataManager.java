package ai.dataeng.sqml.function;

import ai.dataeng.sqml.OperatorType;
import java.util.Optional;
import lombok.Value;

public class FunctionMetadataManager {

  public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle) {
    return null;
  }

  @Value
  public class FunctionMetadata {
    Optional<OperatorType> operatorType;
  }
}

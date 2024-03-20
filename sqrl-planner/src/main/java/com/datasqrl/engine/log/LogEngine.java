package com.datasqrl.engine.log;

import com.datasqrl.engine.ExecutionEngine;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataTypeField;

public interface LogEngine extends ExecutionEngine {

  Log createLog(String logId, RelDataTypeField schema, List<String> primaryKey,
      Optional<String> timestamp);

}

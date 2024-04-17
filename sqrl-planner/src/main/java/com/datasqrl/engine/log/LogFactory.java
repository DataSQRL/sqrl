package com.datasqrl.engine.log;

import com.datasqrl.engine.log.LogEngine.Timestamp;
import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeField;

public interface LogFactory {

  Log create(String logId, RelDataTypeField schema, List<String> primaryKey,
      Timestamp timestamp);
}

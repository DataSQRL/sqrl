package com.datasqrl.engine.log;

import com.datasqrl.engine.log.LogEngine.Timestamp;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;

public interface LogFactory {

  Log create(String logId, String logName, RelDataType schema, List<String> primaryKey,
      Timestamp timestamp);
}

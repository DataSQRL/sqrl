package com.datasqrl.time;

import com.datasqrl.function.FlinkTypeUtil;
import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

import java.time.Instant;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * Converts the timestamp to an ISO timestamp string
 */
@AutoService(AutoRegisterSystemFunction.class)
public class TimestampToString extends ScalarFunction implements AutoRegisterSystemFunction {

  public String eval(Instant instant) {
    return instant.toString();
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return FlinkTypeUtil.basicNullInference(DataTypes.STRING(),
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
  }
}

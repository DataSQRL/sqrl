package com.datasqrl.time;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.datasqrl.function.FlinkTypeUtil;
import com.google.auto.service.AutoService;

/**
 * Returns the timestamp at the given timezone.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class AtZone extends ScalarFunction implements AutoRegisterSystemFunction {

  public ZonedDateTime eval(Instant instant, String zoneId) {
    return instant.atZone(ZoneId.of(zoneId));
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder()
        .typedArguments(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.STRING())
        .outputTypeStrategy(callContext -> {
          var type = FlinkTypeUtil.getFirstArgumentType(callContext);
          if (type.getLogicalType().isNullable()) {
            return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
          }

          return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        }).build();
  }
}
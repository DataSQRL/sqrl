package com.datasqrl.time;

import java.time.Instant;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.datasqrl.function.FlinkTypeUtil;

import lombok.AllArgsConstructor;

@AllArgsConstructor
  public abstract class AbstractTimestampToEpoch extends ScalarFunction implements AutoRegisterSystemFunction {

    private final boolean isMilli;

    public Long eval(Instant instant) {
      var epoch = instant.toEpochMilli();
      if (!isMilli) {
        epoch = epoch / 1000;
      }
      return epoch;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return FlinkTypeUtil.basicNullInference(DataTypes.BIGINT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
//
//    @Override
//    public String getDocumentation() {
//      Instant DEFAULT_DOC_TIMESTAMP = Instant.parse("2023-03-12T18:23:34.083Z");
//      String functionCall = String.format("%s(%s(%s))",
//          getFunctionName(),
//          STRING_TO_TIMESTAMP.getFunctionName(),
//          DEFAULT_DOC_TIMESTAMP.toString());
//      String result = this.eval(DEFAULT_DOC_TIMESTAMP).toString();
//      return String.format("Returns the %s since epoch for the given timestamp.<br />E.g. `%s` returns the number `%s`",
//          isMilli?"milliseconds":"seconds",
//          functionCall, result);
//    }

  }

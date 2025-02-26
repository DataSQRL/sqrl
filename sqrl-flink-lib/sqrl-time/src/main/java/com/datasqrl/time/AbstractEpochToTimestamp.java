package com.datasqrl.time;

import com.datasqrl.function.FlinkTypeUtil;
import java.time.Instant;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

@AllArgsConstructor
public class AbstractEpochToTimestamp extends ScalarFunction {

  boolean isMilli;

  public Instant eval(Long l) {
    if (isMilli) {
      return Instant.ofEpochMilli(l.longValue());
    } else {
      return Instant.ofEpochSecond(l.longValue());
    }
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return FlinkTypeUtil.basicNullInference(
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.BIGINT());
  }
  //
  //  @Override
  //  public String getDocumentation() {
  //    Instant inst = DEFAULT_DOC_TIMESTAMP.truncatedTo(ChronoUnit.SECONDS);
  //    long epoch = inst.toEpochMilli() / (isMilli ? 1 : 1000);
  //    String functionCall = String.format("%s(%s)", getFunctionName(), epoch);
  //    String result = this.eval(epoch).toString();
  //    return String.format(
  //        "Converts the epoch timestamp in %s to the corresponding timestamp.<br />E.g. `%s`
  // returns the timestamp `%s`",
  //        isMilli ? "milliseconds" : "seconds", functionCall, result);
  //  }

}

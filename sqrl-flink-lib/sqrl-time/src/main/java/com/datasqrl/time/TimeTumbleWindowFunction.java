package com.datasqrl.time;

import com.datasqrl.function.FlinkTypeUtil;
import com.datasqrl.function.FlinkTypeUtil.VariableArguments;
import com.datasqrl.function.AutoRegisterSystemFunction;

//import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

@AllArgsConstructor
public abstract class TimeTumbleWindowFunction extends ScalarFunction implements
    TimeTumbleWindowFunctionEval, AutoRegisterSystemFunction {

  protected final ChronoUnit timeUnit;
  protected final ChronoUnit offsetUnit;

  public ChronoUnit getTimeUnit() {
    return timeUnit;
  }

  public ChronoUnit getOffsetUnit() {
    return offsetUnit;
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder().inputTypeStrategy(
            VariableArguments.builder()
                .staticType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                .variableType(DataTypes.BIGINT()).minVariableArguments(0).maxVariableArguments(2)
                .build())
        .outputTypeStrategy(FlinkTypeUtil.nullPreservingOutputStrategy(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))
        .build();
  }

  @Override
  public Instant eval(Instant instant, Long multiple, Long offset) {
    if (multiple == null) {
      multiple = 1L;
    }
//    Preconditions.checkArgument(multiple > 0, "Window width must be positive: %s", multiple);
    if (offset == null) {
      offset = 0L;
    }
//    Preconditions.checkArgument(offset >= 0, "Invalid window offset: %s", offset);
//    Preconditions.checkArgument(
//        offsetUnit.getDuration().multipliedBy(offset).compareTo(timeUnit.getDuration()) < 0,
//        "Offset of %s %s is larger than %s", offset, offsetUnit, timeUnit);

    ZonedDateTime time = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
    ZonedDateTime truncated = time.minus(offset, offsetUnit).truncatedTo(timeUnit);

    long multipleToAdd = 1;
    if (multiple > 1) {
      ZonedDateTime truncatedBase = truncated.with(TemporalAdjusters.firstDayOfYear())
          .truncatedTo(ChronoUnit.DAYS);
      ZonedDateTime timeBase = time.with(TemporalAdjusters.firstDayOfYear())
          .truncatedTo(ChronoUnit.DAYS);
      if (!timeBase.equals(truncatedBase)) {
        //We slipped into the prior base unit (i.e. year) due to offset.
        return timeBase.plus(offset, offsetUnit).minusNanos(1).toInstant();
      }
      Duration timeToBase = Duration.between(truncatedBase, truncated);
      long numberToBase = timeToBase.dividedBy(timeUnit.getDuration());
      multipleToAdd = multiple - (numberToBase % multiple);
    }

    return truncated.plus(multipleToAdd, timeUnit).plus(offset, offsetUnit).minusNanos(1)
        .toInstant();
  }
}

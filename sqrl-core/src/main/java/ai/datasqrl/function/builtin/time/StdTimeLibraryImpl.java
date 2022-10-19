package ai.datasqrl.function.builtin.time;

import ai.datasqrl.function.SqrlFunction;
import ai.datasqrl.function.SqrlTimeTumbleFunction;
import ai.datasqrl.function.TimestampPreservingFunction;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.utils.AdaptedCallContext;

public class StdTimeLibraryImpl {
  public static final NOW NOW = new NOW();
  public static final EPOCH_TO_TIMESTAMP EPOCH_TO_TIMESTAMP = new EPOCH_TO_TIMESTAMP();
  public static final TIMESTAMP_TO_EPOCH TIMESTAMP_TO_EPOCH = new TIMESTAMP_TO_EPOCH();
  public static final STRING_TO_TIMESTAMP STRING_TO_TIMESTAMP = new STRING_TO_TIMESTAMP();
  public static final TIMESTAMP_TO_STRING TIMESTAMP_TO_STRING = new TIMESTAMP_TO_STRING();
  public static final TO_UTC TO_UTC = new TO_UTC();
  public static final AT_ZONE AT_ZONE = new AT_ZONE();
  public static final ROUND_TO_SECOND ROUND_TO_SECOND = new ROUND_TO_SECOND();
  public static final ROUND_TO_MINUTE ROUND_TO_MINUTE = new ROUND_TO_MINUTE();
  public static final ROUND_TO_HOUR ROUND_TO_HOUR = new ROUND_TO_HOUR();
  public static final ROUND_TO_DAY ROUND_TO_DAY = new ROUND_TO_DAY();
  public static final ROUND_TO_MONTH ROUND_TO_MONTH = new ROUND_TO_MONTH();
  public static final ROUND_TO_YEAR ROUND_TO_YEAR = new ROUND_TO_YEAR();
  public static final GET_SECOND GET_SECOND = new GET_SECOND();
  public static final GET_MINUTE GET_MINUTE = new GET_MINUTE();
  public static final GET_HOUR GET_HOUR = new GET_HOUR();
  public static final GET_DAY_OF_WEEK GET_DAY_OF_WEEK = new GET_DAY_OF_WEEK();
  public static final GET_DAY_OF_MONTH GET_DAY_OF_MONTH = new GET_DAY_OF_MONTH();
  public static final GET_DAY_OF_YEAR GET_DAY_OF_YEAR = new GET_DAY_OF_YEAR();
  public static final GET_MONTH GET_MONTH = new GET_MONTH();
  public static final GET_YEAR GET_YEAR = new GET_YEAR();

  public static final List<FlinkFnc> fncs = List.of(
      new FlinkFnc(NOW.class.getSimpleName(), NOW),
      new FlinkFnc(EPOCH_TO_TIMESTAMP.class.getSimpleName(), EPOCH_TO_TIMESTAMP),
      new FlinkFnc(TIMESTAMP_TO_EPOCH.class.getSimpleName(), TIMESTAMP_TO_EPOCH),
      new FlinkFnc(STRING_TO_TIMESTAMP.class.getSimpleName(), STRING_TO_TIMESTAMP),
      new FlinkFnc(TIMESTAMP_TO_STRING.class.getSimpleName(), TIMESTAMP_TO_STRING),
      new FlinkFnc(TO_UTC.class.getSimpleName(), TO_UTC),
      new FlinkFnc(AT_ZONE.class.getSimpleName(), AT_ZONE),
      new FlinkFnc(ROUND_TO_SECOND.class.getSimpleName(), ROUND_TO_SECOND),
      new FlinkFnc(ROUND_TO_MINUTE.class.getSimpleName(), ROUND_TO_MINUTE),
      new FlinkFnc(ROUND_TO_HOUR.class.getSimpleName(), ROUND_TO_HOUR),
      new FlinkFnc(ROUND_TO_DAY.class.getSimpleName(), ROUND_TO_DAY),
      new FlinkFnc(ROUND_TO_MONTH.class.getSimpleName(), ROUND_TO_MONTH),
      new FlinkFnc(ROUND_TO_YEAR.class.getSimpleName(), ROUND_TO_YEAR),
      new FlinkFnc(GET_SECOND.class.getSimpleName(), GET_SECOND),
      new FlinkFnc(GET_MINUTE.class.getSimpleName(), GET_MINUTE),
      new FlinkFnc(GET_HOUR.class.getSimpleName(), GET_HOUR),
      new FlinkFnc(GET_DAY_OF_WEEK.class.getSimpleName(), GET_DAY_OF_WEEK),
      new FlinkFnc(GET_DAY_OF_MONTH.class.getSimpleName(), GET_DAY_OF_MONTH),
      new FlinkFnc(GET_DAY_OF_YEAR.class.getSimpleName(), GET_DAY_OF_YEAR),
      new FlinkFnc(GET_MONTH.class.getSimpleName(), GET_DAY_OF_YEAR),
      new FlinkFnc(GET_YEAR.class.getSimpleName(), GET_YEAR)
  );

  public static class GET_SECOND extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getSecond();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class GET_MINUTE extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getMinute();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class GET_HOUR extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getHour();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

  }

  public static class GET_DAY_OF_WEEK extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfWeek().getValue();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class GET_DAY_OF_MONTH extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfMonth();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class GET_DAY_OF_YEAR extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfYear();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class GET_MONTH extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getMonthValue();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class GET_YEAR extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getYear();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class RoundingFunction extends ScalarFunction implements SqrlTimeTumbleFunction {

    private final ChronoUnit timeUnit;

    public RoundingFunction(ChronoUnit timeUnit) {
      this.timeUnit = timeUnit;
    }

    @Override
    public Specification getSpecification(long[] arguments) {
      Preconditions.checkArgument(arguments.length == 0);
      return new Specification();
    }

    private class Specification implements SqrlTimeTumbleFunction.Specification {

      @Override
      public long getBucketWidthMillis() {
        return timeUnit.getDuration().toMillis();
      }
    }

  }

  public static class ROUND_TO_SECOND extends RoundingFunction {
    public ROUND_TO_SECOND() {
      super(ChronoUnit.SECONDS);
    }
    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS)
          .toInstant();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class ROUND_TO_MINUTE extends RoundingFunction {
    public ROUND_TO_MINUTE() {
      super(ChronoUnit.MINUTES);
    }
    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES)
          .toInstant();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class ROUND_TO_HOUR extends RoundingFunction {
    public ROUND_TO_HOUR() {
      super(ChronoUnit.HOURS);
    }
    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS)
          .toInstant();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class ROUND_TO_DAY extends RoundingFunction {
    public ROUND_TO_DAY() {
      super(ChronoUnit.DAYS);
    }
    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS)
          .toInstant();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class ROUND_TO_MONTH extends RoundingFunction {

    public ROUND_TO_MONTH() {
      super(ChronoUnit.MONTHS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS).toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class ROUND_TO_YEAR extends RoundingFunction {

    public ROUND_TO_YEAR() {
      super(ChronoUnit.YEARS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS).toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class AT_ZONE extends ScalarFunction implements TimestampPreservingFunction{

    public ZonedDateTime eval(Instant instant, String zoneId) {
      return instant.atZone(ZoneId.of(zoneId));
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.STRING())
          .outputTypeStrategy(callContext -> {
            DataType type = getFirstArgumentType(callContext);
            if (type.getLogicalType().isNullable()) {
              return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
            }

            return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
          })
          .build();
    }
  }

  public static class TO_UTC extends ScalarFunction implements TimestampPreservingFunction {

    public Instant eval(ZonedDateTime zonedDateTime) {
      return zonedDateTime.toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class TIMESTAMP_TO_STRING extends ScalarFunction implements SqrlFunction {

    public String eval(Instant instant) {
      return instant.toString();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.STRING(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class STRING_TO_TIMESTAMP extends ScalarFunction implements SqrlFunction {

    public Instant eval(String s) {
      return Instant.parse(s);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.STRING());
    }
  }

  public static class TIMESTAMP_TO_EPOCH extends ScalarFunction implements SqrlFunction {

    public Long eval(Instant instant) {
      return instant.toEpochMilli();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.BIGINT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class EPOCH_TO_TIMESTAMP extends ScalarFunction implements TimestampPreservingFunction {
    public Instant eval(Long l) {
      return Instant.ofEpochSecond(l.longValue());
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), DataTypes.BIGINT());
    }
  }

  public static class NOW extends ScalarFunction implements SqrlFunction {

    public Instant eval() {
      return Instant.now();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .typedArguments()
          .outputTypeStrategy(callContext -> {
            return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull());
          })
          .build();
    }
  }

  public static TypeInference basicNullInference(DataType outputType, DataType inputType) {
    return TypeInference.newBuilder()
        .typedArguments(inputType)
        .outputTypeStrategy(callContext -> {
          DataType type = getFirstArgumentType(callContext);

          if (type.getLogicalType().isNullable()) {
            return Optional.of(outputType.nullable());
          }

          return Optional.of(outputType.notNull());
        })
        .build();
  }

  public static DataType getFirstArgumentType(CallContext callContext) {
    if (callContext instanceof AdaptedCallContext) {
      return ((AdaptedCallContext) callContext).getOriginalContext().getArgumentDataTypes()
          .get(0);
    } else {
      return callContext.getArgumentDataTypes().get(0);
    }
  }

  @Value
  public static class FlinkFnc {

    String name;
    UserDefinedFunction fnc;
  }
}

package ai.datasqrl.function.builtin.time;

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
import org.apache.flink.table.types.inference.TypeInference;

public class StdTimeLibraryImpl {

  public static final List<FlinkFnc> fncs = List.of(
      new FlinkFnc(NOW.class.getSimpleName(), new NOW()),
      new FlinkFnc(EPOCH_TO_TIMESTAMP.class.getSimpleName(), new EPOCH_TO_TIMESTAMP()),
      new FlinkFnc(TIMESTAMP_TO_EPOCH.class.getSimpleName(), new TIMESTAMP_TO_EPOCH()),
      new FlinkFnc(STRING_TO_TIMESTAMP.class.getSimpleName(), new STRING_TO_TIMESTAMP()),
      new FlinkFnc(TIMESTAMP_TO_STRING.class.getSimpleName(), new TIMESTAMP_TO_STRING()),
      new FlinkFnc(TO_UTC.class.getSimpleName(), new TO_UTC()),
      new FlinkFnc(AT_ZONE.class.getSimpleName(), new AT_ZONE()),
      new FlinkFnc(ROUND_TO_SECOND.class.getSimpleName(), new ROUND_TO_SECOND()),
      new FlinkFnc(ROUND_TO_MINUTE.class.getSimpleName(), new ROUND_TO_MINUTE()),
      new FlinkFnc(ROUND_TO_HOUR.class.getSimpleName(), new ROUND_TO_HOUR()),
      new FlinkFnc(ROUND_TO_DAY.class.getSimpleName(), new ROUND_TO_DAY()),
      new FlinkFnc(ROUND_TO_MONTH.class.getSimpleName(), new ROUND_TO_MONTH()),
      new FlinkFnc(ROUND_TO_YEAR.class.getSimpleName(), new ROUND_TO_YEAR()),
      new FlinkFnc(GET_SECOND.class.getSimpleName(), new GET_SECOND()),
      new FlinkFnc(GET_MINUTE.class.getSimpleName(), new GET_MINUTE()),
      new FlinkFnc(GET_HOUR.class.getSimpleName(), new GET_HOUR()),
      new FlinkFnc(GET_DAY_OF_WEEK.class.getSimpleName(), new GET_DAY_OF_WEEK()),
      new FlinkFnc(GET_DAY_OF_MONTH.class.getSimpleName(), new GET_DAY_OF_MONTH()),
      new FlinkFnc(GET_DAY_OF_YEAR.class.getSimpleName(), new GET_DAY_OF_YEAR()),
      new FlinkFnc(GET_MONTH.class.getSimpleName(), new GET_DAY_OF_YEAR()),
      new FlinkFnc(GET_YEAR.class.getSimpleName(), new GET_YEAR())
  );

  public static class GET_SECOND extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getSecond();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class GET_MINUTE extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getMinute();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class GET_HOUR extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getHour();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }

  }

  public static class GET_DAY_OF_WEEK extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfWeek().getValue();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class GET_DAY_OF_MONTH extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfMonth();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class GET_DAY_OF_YEAR extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfYear();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class GET_MONTH extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getMonthValue();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class GET_YEAR extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getYear();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class ROUND_TO_SECOND extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS)
          .toInstant();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class ROUND_TO_MINUTE extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES)
          .toInstant();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class ROUND_TO_HOUR extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS)
          .toInstant();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class ROUND_TO_DAY extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS)
          .toInstant();
    }
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class ROUND_TO_MONTH extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS).toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class ROUND_TO_YEAR extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS).toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class AT_ZONE extends ScalarFunction {

    public ZonedDateTime eval(Instant instant, String zoneId) {
      return instant.atZone(ZoneId.of(zoneId));
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), DataTypes.STRING())
          .outputTypeStrategy(callContext -> {
            DataType type = callContext.getArgumentDataTypes().get(0);
            if (type.getLogicalType().isNullable()) {
              return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
            }

            return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
          })
          .build();
    }
    //todo
  }

  public static class TO_UTC extends ScalarFunction {

    public Instant eval(ZonedDateTime zonedDateTime) {
      return zonedDateTime.toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class TIMESTAMP_TO_STRING extends ScalarFunction {

    public String eval(Instant instant) {
      return instant.toString();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.STRING(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class STRING_TO_TIMESTAMP extends ScalarFunction {

    public Instant eval(String s) {
      return Instant.parse(s);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.STRING(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class TIMESTAMP_TO_EPOCH extends ScalarFunction {

    public Long eval(Instant instant) {
      return instant.toEpochMilli();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.BIGINT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
  }

  public static class EPOCH_TO_TIMESTAMP extends ScalarFunction {
    public Instant eval(Long l) {
      return Instant.ofEpochSecond(l.longValue());
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), DataTypes.BIGINT());
    }
  }

  public static class NOW extends ScalarFunction {

    public Instant eval() {
      return Instant.now();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .typedArguments()
          .outputTypeStrategy(callContext -> {
            return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull());
          })
          .build();
    }
  }

  public static TypeInference basicNullInference(DataType outputType, DataType inputType) {
    return TypeInference.newBuilder()
        .typedArguments(inputType)
        .outputTypeStrategy(callContext -> {
          DataType type = callContext.getArgumentDataTypes().get(0);
          if (type.getLogicalType().isNullable()) {
            return Optional.of(outputType.nullable());
          }

          return Optional.of(outputType.notNull());
        })
        .build();
  }
  
  @Value
  public static class FlinkFnc {

    String name;
    UserDefinedFunction fnc;
  }
}

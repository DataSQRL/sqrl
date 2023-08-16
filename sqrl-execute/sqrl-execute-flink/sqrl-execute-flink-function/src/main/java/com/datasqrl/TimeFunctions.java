package com.datasqrl;

import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.function.SqrlFunction;
import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.function.TimestampPreservingFunction;
import com.datasqrl.util.StringUtil;
import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.inference.utils.AdaptedCallContext;

public class TimeFunctions {
  //
  public static final NOW NOW = new NOW();
  public static final EpochToTimestamp EPOCH_TO_TIMESTAMP = new EpochToTimestamp();
  public static final EpochMilliToTimestamp EPOCH_MILLI_TO_TIMESTAMP = new EpochMilliToTimestamp();
  public static final TimestampToEpoch TIMESTAMP_TO_EPOCH = new TimestampToEpoch();
  public static final TimestampToEpochMilli TIMESTAMP_TO_EPOCH_MILLI = new TimestampToEpochMilli();
  public static final ParseTimestamp STRING_TO_TIMESTAMP = new ParseTimestamp();
  public static final TimestampToString TIMESTAMP_TO_STRING = new TimestampToString();
  public static final AtZone AT_ZONE = new AtZone();
  public static final EndOfSecond END_OF_SECOND = new EndOfSecond();
  public static final EndOfMinute END_OF_MINUTE = new EndOfMinute();
  public static final EndOfHour END_OF_HOUR = new EndOfHour();
  public static final EndOfDay END_OF_DAY = new EndOfDay();
  public static final EndOfWeek END_OF_WEEK = new EndOfWeek();
  public static final EndOfMonth END_OF_MONTH = new EndOfMonth();
  public static final EndOfYear END_OF_YEAR = new EndOfYear();

  public static final EndOfInterval END_OF_INTERVAL = new EndOfInterval();

  private static final Instant DEFAULT_DOC_TIMESTAMP = Instant.parse("2023-03-12T18:23:34.083Z");



  public static class EndOfInterval extends ScalarFunction implements SqrlFunction,
      SqrlTimeTumbleFunction {

    @Override
    public String getDocumentation() {
      return "Rounds timestamp to the end of the given interval and offset.";
    }

    public Instant eval(Instant instant, Integer width, Integer offset) {
      throw new NotYetImplementedException("testing");
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
              DataTypes.INT(), DataTypes.INT())
          .outputTypeStrategy(nullPreservingOutputStrategy(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))
          .build();
    }

    @Override
    public Specification getSpecification(long[] arguments) {
      Preconditions.checkArgument(arguments.length == 2);
      return new Specification() {
        @Override
        public long getWindowWidthMillis() {
          return arguments[0]*1000;
        }

        @Override
        public long getWindowOffsetMillis() {
          return arguments[1]*1000;
        }
      };
    }

  }

  private interface TimeWindowBucketFunctionEval {

    Instant eval(Instant instant);

  }

  public abstract static class TimeWindowBucketFunction extends ScalarFunction implements SqrlFunction,
      SqrlTimeTumbleFunction, TimeWindowBucketFunctionEval {

    protected final ChronoUnit timeUnit;

    public TimeWindowBucketFunction(ChronoUnit timeUnit) {
      this.timeUnit = timeUnit;
    }

    @Override
    public String getDocumentation() {
      Instant DEFAULT_DOC_TIMESTAMP = Instant.parse("2023-03-12T18:23:34.083Z");
      String functionCall = String.format("%s(%s(%s))",
          getFunctionName().getDisplay(),
          STRING_TO_TIMESTAMP.getFunctionName().getDisplay(),
          DEFAULT_DOC_TIMESTAMP.toString());
      String result = this.eval(DEFAULT_DOC_TIMESTAMP).toString();
      return String.format("Time window function that returns the end of %s for the timestamp argument.<br />E.g. `%s` returns the timestamp `%s`",
          StringUtil.removeFromEnd(timeUnit.toString().toLowerCase(),"s"), functionCall, result);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

    @Override
    public Specification getSpecification(long[] arguments) {
      Preconditions.checkArgument(arguments.length == 0);
      return new Specification();
    }

    private class Specification implements SqrlTimeTumbleFunction.Specification {

      @Override
      public long getWindowWidthMillis() {
        return timeUnit.getDuration().toMillis();
      }

      @Override
      public long getWindowOffsetMillis() {
        return 0;
      }
    }

  }

  public static class EndOfSecond extends TimeWindowBucketFunction {

    public EndOfSecond() {
      super(ChronoUnit.SECONDS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(timeUnit)
          .plus(1,timeUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }


  }

  public static class EndOfMinute extends TimeWindowBucketFunction {

    public EndOfMinute() {
      super(ChronoUnit.MINUTES);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(timeUnit)
          .plus(1,timeUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

  }

  public static class EndOfHour extends TimeWindowBucketFunction {

    public EndOfHour() {
      super(ChronoUnit.HOURS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(timeUnit)
          .plus(1,timeUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

  }

  public static class EndOfDay extends TimeWindowBucketFunction {

    public EndOfDay() {
      super(ChronoUnit.DAYS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(timeUnit)
          .plus(1,timeUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }


  }

  public static class EndOfWeek extends TimeWindowBucketFunction {

    public EndOfWeek() {
      super(ChronoUnit.WEEKS);
    }

    public Instant eval(Instant instant) {
      ZonedDateTime time = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
      int daysToSubtract = time.getDayOfWeek().getValue()-1;
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS)
          .minus(daysToSubtract, ChronoUnit.DAYS)
          .plus(1,timeUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }


  }

  public static class EndOfMonth extends TimeWindowBucketFunction {

    public EndOfMonth() {
      super(ChronoUnit.MONTHS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfNextMonth()).truncatedTo(ChronoUnit.DAYS)
          .minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

  }

  public static class EndOfYear extends TimeWindowBucketFunction {

    public EndOfYear() {
      super(ChronoUnit.YEARS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfNextYear()).truncatedTo(ChronoUnit.DAYS)
          .minus(1, ChronoUnit.NANOS)
          .toInstant();
    }


  }

  public static class AtZone extends ScalarFunction implements SqrlFunction,
      TimestampPreservingFunction {

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


    @Override
    public String getDocumentation() {
      return String.format("Returns the timestamp at the given timezone.");
    }
  }

  public static class TimestampToString extends ScalarFunction implements SqrlFunction {

    public String eval(Instant instant) {
      return instant.toString();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.STRING(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }


    @Override
    public String getDocumentation() {
      return String.format("Converts the timestamp to an ISO timestamp string");
    }
  }

  public static class ParseTimestamp extends ScalarFunction implements SqrlFunction {

    public Instant eval(String s) {
      return Instant.parse(s);
    }

    public Instant eval(String s, String format) {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format, Locale.US);
      return LocalDateTime.parse(s, formatter)
          .atZone(ZoneId.systemDefault())
          .toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .inputTypeStrategy(stringToTimestampInputTypeStrategy())
//          .typedArguments(DataTypes.STRING(), DataTypes.STRING().nullable())
          .outputTypeStrategy(
              nullPreservingOutputStrategy(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))
          .build();
    }


    @Override
    public String getDocumentation() {
      return String.format("Parses a timestamp from an ISO timestamp string.");
    }
  }

  public static InputTypeStrategy stringToTimestampInputTypeStrategy() {
    return new InputTypeStrategy() {

      @Override
      public ArgumentCount getArgumentCount() {
        return new ArgumentCount() {
          @Override
          public boolean isValidCount(int count) {
            return count == 1 || count == 2;
          }

          @Override
          public Optional<Integer> getMinCount() {
            return Optional.of(1);
          }

          @Override
          public Optional<Integer> getMaxCount() {
            return Optional.of(2);
          }
        };
      }

      @Override
      public Optional<List<DataType>> inferInputTypes(CallContext callContext,
          boolean throwOnFailure) {
        if (callContext.getArgumentDataTypes().size() == 1) {
          return Optional.of(List.of(DataTypes.STRING()));
        } else if (callContext.getArgumentDataTypes().size() == 2) {
          return Optional.of(List.of(DataTypes.STRING(), DataTypes.STRING()));
        }

        return Optional.empty();
      }

      @Override
      public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return List.of(Signature.of(Signature.Argument.of("STRING"),
            Signature.Argument.of("STRING")));
      }
    };
  }

  @AllArgsConstructor
  private abstract static class AbstractTimestampToEpoch extends ScalarFunction implements SqrlFunction {

    private final boolean isMilli;

    public Long eval(Instant instant) {
      long epoch = instant.toEpochMilli();
      if (!isMilli) epoch = epoch/1000;
      return epoch;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.BIGINT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

    @Override
    public String getDocumentation() {
      Instant DEFAULT_DOC_TIMESTAMP = Instant.parse("2023-03-12T18:23:34.083Z");
      String functionCall = String.format("%s(%s(%s))",
          getFunctionName().getDisplay(),
          STRING_TO_TIMESTAMP.getFunctionName().getDisplay(),
          DEFAULT_DOC_TIMESTAMP.toString());
      String result = this.eval(DEFAULT_DOC_TIMESTAMP).toString();
      return String.format("Returns the %s since epoch for the given timestamp.<br />E.g. `%s` returns the number `%s`",
          isMilli?"milliseconds":"seconds",
          functionCall, result);
    }

  }

  public static class TimestampToEpoch extends AbstractTimestampToEpoch {

    public TimestampToEpoch() {
      super(false);
    }
  }

  public static class TimestampToEpochMilli extends AbstractTimestampToEpoch {

    public TimestampToEpochMilli() {
      super(true);
    }
  }


  @AllArgsConstructor
  public static class AbstractEpochToTimestamp extends ScalarFunction implements SqrlFunction {

    boolean isMilli;


    public Instant eval(Long l) {
      if (isMilli) return Instant.ofEpochMilli(l.longValue());
      else return Instant.ofEpochSecond(l.longValue());
    }


    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.BIGINT());
    }

    @Override
    public String getDocumentation() {
      Instant inst = DEFAULT_DOC_TIMESTAMP.truncatedTo(ChronoUnit.SECONDS);
      long epoch = inst.toEpochMilli()/(isMilli?1:1000);
      String functionCall = String.format("%s(%s)",
          getFunctionName().getDisplay(), epoch);
      String result = this.eval(epoch).toString();
      return String.format("Converts the epoch timestamp in %s to the corresponding timestamp.<br />E.g. `%s` returns the timestamp `%s`",
          isMilli?"milliseconds":"seconds",
          functionCall, result);
    }

  }

  public static class EpochToTimestamp extends AbstractEpochToTimestamp {

    public EpochToTimestamp() {
      super(false);
    }

  }

  public static class EpochMilliToTimestamp extends AbstractEpochToTimestamp {

    public EpochMilliToTimestamp() {
      super(true);
    }
  }

  public static class NOW extends ScalarFunction implements SqrlFunction {

    public Instant eval() {
      return Instant.now();
    }


    @Override
    public String getDocumentation() {
      return "Special timestamp function that evaluates to the current time on the timeline.";
    }
  }


  public static TypeStrategy nullPreservingOutputStrategy(DataType outputType) {
    return callContext -> {
      DataType type = getFirstArgumentType(callContext);

      if (type.getLogicalType().isNullable()) {
        return Optional.of(outputType.nullable());
      }

      return Optional.of(outputType.notNull());
    };
  }

  public static TypeInference basicNullInference(DataType outputType, DataType inputType) {
    return TypeInference.newBuilder()
        .typedArguments(inputType)
        .outputTypeStrategy(nullPreservingOutputStrategy(outputType))
        .build();
  }

  @SneakyThrows
  public static DataType getFirstArgumentType(CallContext callContext) {
    if (callContext instanceof AdaptedCallContext) {
      Field privateField = AdaptedCallContext.class.getDeclaredField("originalContext");
      privateField.setAccessible(true);
      CallContext originalContext = (CallContext) privateField.get(callContext);

      return originalContext
          .getArgumentDataTypes()
          .get(0);
    } else {
      return callContext.getArgumentDataTypes().get(0);
    }
  }



}

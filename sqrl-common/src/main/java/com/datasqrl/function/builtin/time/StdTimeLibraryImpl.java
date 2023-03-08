/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function.builtin.time;

import com.datasqrl.function.SqrlFunction;
import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.function.TimestampPreservingFunction;
import com.datasqrl.function.builtin.FunctionUtil;
import com.datasqrl.loaders.SqrlModule;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.calcite.TypeFactory;
import com.datasqrl.plan.local.generate.FlinkUdfNsObject;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
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
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;
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

@Getter
public class StdTimeLibraryImpl implements SqrlModule {

  //
  public static final NOW NOW = new NOW();
  public static final EpochToTimestamp EPOCH_TO_TIMESTAMP = new EpochToTimestamp();
  public static final EpochMilliToTimestamp EPOCH_MILLI_TO_TIMESTAMP = new EpochMilliToTimestamp();
  public static final TimestampToEpoch TIMESTAMP_TO_EPOCH = new TimestampToEpoch();
  public static final TimestampToEpochMilli TIMESTAMP_TO_EPOCH_MILLI = new TimestampToEpochMilli();
  public static final ParseTimestamp STRING_TO_TIMESTAMP = new ParseTimestamp();
  public static final FormatTimestamp TIMESTAMP_TO_STRING = new FormatTimestamp();
  public static final AtZone AT_ZONE = new AtZone();
  public static final EndOfSecond END_OF_SECOND = new EndOfSecond();
  public static final EndOfMinute END_OF_MINUTE = new EndOfMinute();
  public static final EndOfHour END_OF_HOUR = new EndOfHour();
  public static final EndOfDay END_OF_DAY = new EndOfDay();
  public static final EndOfWeek END_OF_WEEK = new EndOfWeek();
  public static final EndOfMonth END_OF_MONTH = new EndOfMonth();
  public static final EndOfYear END_OF_YEAR = new EndOfYear();
  public static final GetSecond GET_SECOND = new GetSecond();
  public static final GetMinute GET_MINUTE = new GetMinute();
  public static final GetHour GET_HOUR = new GetHour();
  public static final GetDayOfWeek GET_DAY_OF_WEEK = new GetDayOfWeek();
  public static final GetDayOfMonth GET_DAY_OF_MONTH = new GetDayOfMonth();
  public static final GetDayOfYear GET_DAY_OF_YEAR = new GetDayOfYear();
  public static final GetMonth GET_MONTH = new GetMonth();
  public static final GetYear GET_YEAR = new GetYear();

  static RelDataTypeFactory sqrlTypeFactory = TypeFactory.getTypeFactory();

  //  @Override
  public NamePath getPath() {
    return NamePath.of("time");
  }

  public SqlOperator lookupFunction(String name) {

    return null;
  }

  public final List<ScalarFunction> functions = List.of(
//      NOW,
      EPOCH_TO_TIMESTAMP,
      EPOCH_MILLI_TO_TIMESTAMP,
      TIMESTAMP_TO_EPOCH,
      STRING_TO_TIMESTAMP,
      TIMESTAMP_TO_STRING,
      AT_ZONE,
      END_OF_SECOND,
      END_OF_MINUTE,
      END_OF_HOUR,
      END_OF_DAY,
      END_OF_WEEK,
      END_OF_MONTH,
      END_OF_YEAR,
      GET_SECOND,
      GET_MINUTE,
      GET_HOUR,
      GET_DAY_OF_WEEK,
      GET_DAY_OF_MONTH,
      GET_DAY_OF_YEAR,
      GET_MONTH,
      GET_YEAR
  );
  List<NamespaceObject> nsObjects = getFunctions().stream()
      .map(f -> new FlinkUdfNsObject(FunctionUtil.getFunctionNameFromClass(f.getClass()),
                                     f, Optional.empty()))
      .collect(Collectors.toList());

  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    return getNamespaceObjects().stream()
        .filter(f -> f.getName().equals(name))
        .findAny();
  }

  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return nsObjects;
  }

  public static class GetSecond extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getSecond();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
//
//    @Override
//    public SqlReturnTypeInference getReturnTypeInference() {
//      return nullPreservingReturnType(sqrlTypeFactory.createSqlType(SqlTypeName.INTEGER));
//    }
//
//    @Override
//    public SqlOperandTypeInference getOperandTypeInference() {
//      return new SqrlExplicitOperandTypeInference(
//          List.of(sqrlTypeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3)));
//    }
//
//    @Override
//    public SqlOperandTypeChecker getOperandTypeChecker() {
//      return OperandTypes.COMPARABLE_ORDERED;
//    }
  }

  public static class GetMinute extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getMinute();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

//    @Override
//    public SqlReturnTypeInference getReturnTypeInference() {
//      return nullPreservingReturnType(sqrlTypeFactory.createSqlType(SqlTypeName.INTEGER));
//    }
//
//    @Override
//    public SqlOperandTypeInference getOperandTypeInference() {
//      return new SqrlExplicitOperandTypeInference(
//          List.of(sqrlTypeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3)));
//    }
//
//    @Override
//    public SqlOperandTypeChecker getOperandTypeChecker() {
//      return OperandTypes.COMPARABLE_ORDERED;
//    }
  }

  public static class GetHour extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getHour();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }
  }

  public static class GetDayOfWeek extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfWeek().getValue();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

  }

  public static class GetDayOfMonth extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfMonth();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

  }

  public static class GetDayOfYear extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfYear();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

  }

  public static class GetMonth extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getMonthValue();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

  }

  public static class GetYear extends ScalarFunction implements SqrlFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getYear();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.INT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

  }

  public abstract static class RoundingFunction extends ScalarFunction implements SqrlFunction,
      SqrlTimeTumbleFunction {

    protected final ChronoUnit timeUnit;

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

  public static class EndOfSecond extends RoundingFunction {

    public EndOfSecond() {
      super(ChronoUnit.SECONDS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(timeUnit)
          .plus(1,timeUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

  }

  public static class EndOfMinute extends RoundingFunction {

    public EndOfMinute() {
      super(ChronoUnit.MINUTES);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(timeUnit)
          .plus(1,timeUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

  }

  public static class EndOfHour extends RoundingFunction {

    public EndOfHour() {
      super(ChronoUnit.HOURS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(timeUnit)
          .plus(1,timeUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

  }

  public static class EndOfDay extends RoundingFunction {

    public EndOfDay() {
      super(ChronoUnit.DAYS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(timeUnit)
          .plus(1,timeUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }


  }

  public static class EndOfWeek extends RoundingFunction {

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

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }


  }

  public static class EndOfMonth extends RoundingFunction {

    public EndOfMonth() {
      super(ChronoUnit.MONTHS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfNextMonth()).truncatedTo(ChronoUnit.DAYS)
          .minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }


  }

  public static class EndOfYear extends RoundingFunction {

    public EndOfYear() {
      super(ChronoUnit.YEARS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfNextYear()).truncatedTo(ChronoUnit.DAYS)
          .minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
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


  }

  public static class FormatTimestamp extends ScalarFunction implements SqrlFunction {

    public String eval(Instant instant) {
      return instant.toString();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.STRING(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
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

  public static class EpochToTimestamp extends ScalarFunction implements SqrlFunction,
      TimestampPreservingFunction {

    public Instant eval(Long l) {
      return Instant.ofEpochSecond(l.longValue());
    }


    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.BIGINT());
    }
  }

  public static class EpochMilliToTimestamp extends ScalarFunction implements SqrlFunction,
          TimestampPreservingFunction {

    public Instant eval(Long l) {
      return Instant.ofEpochMilli(l.longValue());
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.BIGINT());
    }
  }

//
//  public class SqrlScalarTimeFunction extends SqlFunction {
//
//    private final SqlTypeName typeName;
//
//    protected SqrlScalarTimeFunction(String name, SqlTypeName typeName) {
//      super(name, SqlKind.OTHER_FUNCTION, (SqlReturnTypeInference)null, (SqlOperandTypeInference)null,
//          OperandTypes.or(new SqlSingleOperandTypeChecker[]{OperandTypes.POSITIVE_INTEGER_LITERAL, OperandTypes.NILADIC}),
//          SqlFunctionCategory.TIMEDATE);
//      this.typeName = typeName;
//    }
//  }
//
//  @AllArgsConstructor
//  public class Sqrl2FlinkFunctionMapper {
//    RelOptCluster cluster;
//
//    public BridgingSqlFunction map(SqlFunction fnc) {
//      UserDefinedFunction udf = UserDefinedFunctionHelper.instantiateFunction(fnc.getClass());
//
//      ContextResolvedFunction contextResolvedFunction = ContextResolvedFunction.temporary(
//          FunctionIdentifier.of(fnc.getName()),
//          udf);
//
//      return BridgingSqlFunction.of(cluster, contextResolvedFunction);
//    }
//
//  }

  public static class NOW extends ScalarFunction implements SqrlFunction {

    public Instant eval() {
      return Instant.now();
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

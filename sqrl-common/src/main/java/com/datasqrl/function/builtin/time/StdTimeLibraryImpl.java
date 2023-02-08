/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function.builtin.time;

import com.datasqrl.function.SqrlFunction;
import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.function.TimestampPreservingFunction;
import com.datasqrl.loaders.SqrlModule;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.calcite.TypeFactory;
import com.datasqrl.plan.local.generate.FlinkUdfNsObject;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.google.auto.service.AutoService;
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
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqrlExplicitOperandTypeInference;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.internal.FlinkEnvProxy;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
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
  public static final EPOCH_TO_TIMESTAMP EPOCH_TO_TIMESTAMP = new EPOCH_TO_TIMESTAMP();
  public static final TIMESTAMP_TO_EPOCH TIMESTAMP_TO_EPOCH = new TIMESTAMP_TO_EPOCH();
  public static final STRING_TO_TIMESTAMP STRING_TO_TIMESTAMP = new STRING_TO_TIMESTAMP();
  public static final TIMESTAMP_TO_STRING TIMESTAMP_TO_STRING = new TIMESTAMP_TO_STRING();
  public static final TO_UTC TO_UTC = new TO_UTC();
  public static final AT_ZONE AT_ZONE = new AT_ZONE();
  public static final ROUND_TO_SECOND ROUND_TO_SECOND = new ROUND_TO_SECOND();
  public static final ROUND_TO_MINUTE ROUND_TO_MINUTE = new ROUND_TO_MINUTE();
  public static final ROUND_TO_HOUR ROUND_TO_HOUR = new ROUND_TO_HOUR();
  public static final ROUND_TO_WEEK ROUND_TO_WEEK = new ROUND_TO_WEEK();
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
      TIMESTAMP_TO_EPOCH,
      STRING_TO_TIMESTAMP,
      TIMESTAMP_TO_STRING,
      TO_UTC,
      AT_ZONE,
      ROUND_TO_SECOND,
      ROUND_TO_MINUTE,
      ROUND_TO_HOUR,
      ROUND_TO_DAY,
      ROUND_TO_WEEK,
      ROUND_TO_MONTH,
      ROUND_TO_YEAR,
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
      .map(f -> new FlinkUdfNsObject(Name.system(f.getClass().getSimpleName()), f, Optional.empty()))
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

  public static class GET_SECOND extends ScalarFunction implements SqrlFunction {

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

  public static class GET_MINUTE extends ScalarFunction implements SqrlFunction {

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

  public abstract static class RoundingFunction extends ScalarFunction implements SqrlFunction,
      SqrlTimeTumbleFunction {

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
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
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
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
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
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
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
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }


  }

  public static class ROUND_TO_WEEK extends RoundingFunction {

    public ROUND_TO_WEEK() {
      super(ChronoUnit.MONTHS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfMonth())
          .truncatedTo(ChronoUnit.WEEKS).toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
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
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
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
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }


  }

  public static class AT_ZONE extends ScalarFunction implements SqrlFunction,
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

  public static class TO_UTC extends ScalarFunction implements SqrlFunction,
      TimestampPreservingFunction {

    public Instant eval(ZonedDateTime zonedDateTime) {
      return zonedDateTime.toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
          DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
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

  public static class TIMESTAMP_TO_EPOCH extends ScalarFunction implements SqrlFunction {

    public Long eval(Instant instant) {
      return instant.toEpochMilli();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return basicNullInference(DataTypes.BIGINT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }


  }

  public static class EPOCH_TO_TIMESTAMP extends ScalarFunction implements SqrlFunction,
      TimestampPreservingFunction {

    public Instant eval(Long l) {
      return Instant.ofEpochSecond(l.longValue());
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

package ai.dataeng.sqml.function;

import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.schema2.basic.DateTimeType;
import ai.dataeng.sqml.schema2.basic.FloatType;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.schema2.basic.NumberType;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.schema2.basic.UuidType;
import java.util.List;

public class PostgresFunctions {

  public static List<SqmlFunction> SqmlSystemFunctions = List.of(
      new SqmlFunction("echo", new StringType(), false),
      new SqmlFunction("concat", new StringType(), false),
      new SqmlFunction("math.abs", new NumberType(), false),
      new SqmlFunction("count", new IntegerType(), true),
      new SqmlFunction("casts.tointeger", new IntegerType(), false),
      new SqmlFunction("casts.toboolean", new BooleanType(), false),
      new SqmlFunction("casts.tofloat", new FloatType(), false),
      new SqmlFunction("casts.tostring", new StringType(), false),
      new SqmlFunction("casts.touuid", new UuidType(), false),
      new SqmlFunction("casts.totimestamp", new DateTimeType(), false),
      new SqmlFunction("collect_as_set", new ArrayType(new StringType()), false), //todo subtype
      new SqmlFunction("sum", new NumberType(), false),
      new SqmlFunction("roundtoday", new DateTimeType(), false),
      new SqmlFunction("timefct.roundtomin", new DateTimeType(), false), //todo unite
      new SqmlFunction("timefct.roundtohour", new DateTimeType(), false),
      new SqmlFunction("timefct.dayofweek", new DateTimeType(), false),
      new SqmlFunction("timefct.hourofday", new DateTimeType(), false),
      new SqmlFunction("time.roundtoday", new DateTimeType(), false),
      new SqmlFunction("time.dayofweek", new DateTimeType(), false),
      new SqmlFunction("time.roundtoweek", new DateTimeType(), false),
      new SqmlFunction("time.roundtomonth", new DateTimeType(), false),
      new SqmlFunction("avg", new NumberType(), false),
      new SqmlFunction("max", new NumberType(), false),
      new SqmlFunction("iff", new StringType(), false),
      new SqmlFunction("now", new DateTimeType(), false),
      new SqmlFunction("math.log10", new FloatType(), false),
      new SqmlFunction("math.sqrt", new FloatType(), false),
      new SqmlFunction("stats.removeoutlier", new FloatType(), false),
      new SqmlFunction("stats.normal.variance", new FloatType(), false),
      new SqmlFunction("stats.truncated_normal.percentile", new FloatType(), false),
      new SqmlFunction("stats.poisson.percentile", new FloatType(), false),
      new SqmlFunction("time.parse", new FloatType(), false),
      new SqmlFunction("math.floor", new FloatType(), false),
      new SqmlFunction("col.multiply", new FloatType(), false),
      new SqmlFunction("time.roundtomin", new DateTimeType(), false),
      new SqmlFunction("frozen_lookup", new StringType(), false)
  );
}

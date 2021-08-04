package ai.dataeng.sqml.function;

import ai.dataeng.sqml.type.SqmlType;
import java.util.List;

public class PostgresFunctions {

  public static List<SqmlFunction> SqmlSystemFunctions = List.of(
      new SqmlFunction("concat", new SqmlType.StringSqmlType(), false),
      new SqmlFunction("math.abs", new SqmlType.NumberSqmlType(), false),
      new SqmlFunction("count", new SqmlType.IntegerSqmlType(), false),
      new SqmlFunction("casts.tointeger", new SqmlType.IntegerSqmlType(), false),
      new SqmlFunction("casts.tofloat", new SqmlType.FloatSqmlType(), false),
      new SqmlFunction("casts.tostring", new SqmlType.StringSqmlType(), false),
      new SqmlFunction("casts.touuid", new SqmlType.UuidSqmlType(), false),
      new SqmlFunction("casts.totimestamp", new SqmlType.DateTimeSqmlType(), false)
  );
}

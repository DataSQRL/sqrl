package ai.dataeng.sqml.function;

import ai.dataeng.sqml.type.SqmlType;
import java.util.List;

public class PostgresFunctions {

  public static List<SqmlFunction> SqmlSystemFunctions = List.of(
      new SqmlFunction("concat", new SqmlType.StringSqmlType(), false),
      new SqmlFunction("math.abs", new SqmlType.NumberSqmlType(), false)
//      new SqmlFunction("sum", SqmlType.INTEGER, true)
//      new SqmlFunction("count", SqmlType.RELATION, true)
  );
}

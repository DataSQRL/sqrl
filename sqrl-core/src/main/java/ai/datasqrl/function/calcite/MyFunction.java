package ai.datasqrl.function.calcite;

import java.lang.reflect.Method;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

public class MyFunction {
  //Code generation for finding the function implementation
  private static final Method method = Types.lookupMethod(MyFunction.class, "eval", Integer.class);
  //Convert to a calcite function
  public static final ScalarFunction FUNCTION = ScalarFunctionImpl.create(method);

  @SuppressWarnings("unused")
  public int eval(Integer a) {
    return a + 1;
  }
}

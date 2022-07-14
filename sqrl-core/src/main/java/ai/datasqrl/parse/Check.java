package ai.datasqrl.parse;

import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.plan.local.Errors.Error;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Check {

  public static Exception newException() {
    return new SqrlExceptions();
  }

  public static <T> void state(boolean check, T node, Error<T> error) {
    if (!check) {
      throw new RuntimeException(node.toString());
    }
  }
  public static <T> void state_(boolean check, Supplier<T> node, Error<T> error) {
    if (!check) {
      throw new RuntimeException(node.toString());
    }
  }

  public static class SqrlExceptions extends Exception {

  }
}

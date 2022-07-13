package ai.datasqrl.parse;

import ai.datasqrl.plan.local.Errors.Error;

public class Check {

  public static Exception newException() {
    return new SqrlExceptions();
  }

  public static <T> void state(boolean check, T node, Error<T> error) {
    if (check) {
    }
  }

  public static class SqrlExceptions extends Exception {

  }
}

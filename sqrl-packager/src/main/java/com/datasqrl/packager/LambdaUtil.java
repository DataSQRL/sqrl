package com.datasqrl.packager;

import java.util.concurrent.Callable;

public class LambdaUtil {
  public static <T> T rethrowCall(Callable<T> callable) {
    try {
      return callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

package com.datasqrl.cmd;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;

public class AssertStatusHook implements StatusHook {
  public static AssertStatusHook INSTANCE = new AssertStatusHook();
  @Override
  public void onSuccess() {
  }

  @Override
  public void onFailure(Exception e, ErrorCollector errors) {
    System.out.println(ErrorPrinter.prettyPrint(errors));
    fail(e);
  }
}

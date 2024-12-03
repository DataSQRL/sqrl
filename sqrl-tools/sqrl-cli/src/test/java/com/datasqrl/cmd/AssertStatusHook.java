package com.datasqrl.cmd;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import lombok.Getter;

public class AssertStatusHook implements StatusHook {
  private boolean failed;
  @Getter
  private String failMessage = null;

  @Override
  public void onSuccess() {
  }

  @Override
  public void onFailure(Throwable e, ErrorCollector errors) {
    failMessage = ErrorPrinter.prettyPrint(errors);
    failed = true;
  }

  @Override
  public boolean isSuccess() {
    return !failed;
  }

  @Override
  public boolean isFailed() {
    return failed;
  }
}

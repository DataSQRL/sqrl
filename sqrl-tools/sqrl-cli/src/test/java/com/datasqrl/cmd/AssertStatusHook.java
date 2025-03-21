package com.datasqrl.cmd;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import lombok.Getter;

public class AssertStatusHook implements StatusHook {
  private boolean failed;
  @Getter
  private String messages = null;

  @Override
  public void onSuccess(ErrorCollector errors) {
    messages = ErrorPrinter.prettyPrint(errors);
  }

  @Override
  public void onFailure(Throwable e, ErrorCollector errors) {
    messages = ErrorPrinter.prettyPrint(errors);
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

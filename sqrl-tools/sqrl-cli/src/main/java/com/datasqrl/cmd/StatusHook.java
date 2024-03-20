package com.datasqrl.cmd;

import com.datasqrl.error.ErrorCollector;
import lombok.Getter;

public interface StatusHook {

  void onSuccess();

  void onFailure(Exception e, ErrorCollector errors);

  public static final StatusHook NONE = new StatusHook() {
    @Override
    public void onSuccess() {

    }

    @Override
    public void onFailure(Exception e, ErrorCollector errors) {
e.printStackTrace();
    }
  };

  @Getter
  public static class Impl implements StatusHook {

    private boolean success = false;
    private boolean failed = false;

    @Override
    public void onSuccess() {
      success = true;
    }

    @Override
    public void onFailure(Exception e, ErrorCollector errors) {
      failed = true;
    }
  }

}

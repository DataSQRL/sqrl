package com.datasqrl.cmd;

import lombok.Getter;

public interface StatusHook {

  void onSuccess();

  void onFailure();

  public static final StatusHook NONE = new StatusHook() {
    @Override
    public void onSuccess() {

    }

    @Override
    public void onFailure() {

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
    public void onFailure() {
      failed = true;
    }
  }

}

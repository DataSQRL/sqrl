package com.datasqrl.cmd;

import lombok.Getter;

public interface StatusHook {

  void onSuccess();

  void onFailure(Exception e);

  public static final StatusHook NONE = new StatusHook() {
    @Override
    public void onSuccess() {

    }

    @Override
    public void onFailure(Exception e) {

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
    public void onFailure(Exception e) {
      failed = true;
    }
  }

}

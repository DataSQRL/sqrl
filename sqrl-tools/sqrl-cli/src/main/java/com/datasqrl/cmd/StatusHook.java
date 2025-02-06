package com.datasqrl.cmd;

import com.datasqrl.error.ErrorCollector;

import lombok.Getter;

public interface StatusHook {

  void onSuccess();

  void onFailure(Throwable e, ErrorCollector errors);
  boolean isSuccess();

  boolean isFailed();

  public static final StatusHook NONE = new StatusHook() {
    boolean failed = false;
    @Override
    public void onSuccess() {

    }

    @Override
    public void onFailure(Throwable e, ErrorCollector errors) {
      e.printStackTrace();
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
  };

  @Getter
  public static class Impl implements StatusHook {

    private boolean failed = false;

    @Override
    public void onSuccess() {
      failed = false;
    }

    @Override
    public void onFailure(Throwable e, ErrorCollector errors) {
      failed = true;
    }

    @Override
    public boolean isSuccess() {
      return false;
    }

    @Override
	public boolean isFailed() {
      return failed;
    }
  }

}

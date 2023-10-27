package com.datasqrl.frontend;

import com.datasqrl.error.ErrorCollector;

public abstract class SqrlBase {

  protected ErrorCollector errors;

  public SqrlBase(ErrorCollector errors) {
    this.errors = errors;
  }
}

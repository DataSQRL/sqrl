package com.datasqrl.util;

import java.util.Arrays;

public abstract class ServiceLoaderFactoryItem<T> implements ServiceLoaderFactory.Matchable<T> {
  private final Object[] criteria;

  public ServiceLoaderFactoryItem(Object... criteria) {
    this.criteria = criteria;
  }

  @Override
  public boolean match(Object... criteria) {
    return Arrays.equals(this.criteria, criteria);
  }
}

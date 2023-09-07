package com.datasqrl.util;

public interface ServiceLoadableClass<T> {

  T create();

  boolean matches(Object[] args);

}

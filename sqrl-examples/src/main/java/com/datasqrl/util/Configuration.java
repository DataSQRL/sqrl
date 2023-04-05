package com.datasqrl.util;

/**
 * Marker interface to indicate a class contains configuration options
 */
public interface Configuration {

  default void scale(long scaleFactor, long number) {
    //Does nothing
  }

}

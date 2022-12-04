/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

public interface SourceMap {

  String getSource();

  public class EmptySourceMap implements SourceMap {

    @Override
    public String getSource() {
      return "";
    }
  }
}

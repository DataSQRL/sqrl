/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import java.io.Serializable;

public interface SourceMap extends Serializable {

  String getSource();

  public class EmptySourceMap implements SourceMap {

    @Override
    public String getSource() {
      return "";
    }
  }
}

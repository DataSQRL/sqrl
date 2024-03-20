/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import com.datasqrl.error.ErrorLocation.FileRange;
import java.io.Serializable;

public interface SourceMap extends Serializable {

  String getSource();

  String getRange(FileRange range);

  public class EmptySourceMap implements SourceMap {

    @Override
    public String getSource() {
      return "";
    }

    @Override
    public String getRange(FileRange range) {
      return "";
    }


  }
}

package com.datasqrl.io.impl.file;

import com.datasqrl.io.DataSystemConnector;

public class FileDataSystemConnector implements DataSystemConnector {

  @Override
  public boolean hasSourceTimestamp() {
    return false;
  }


}

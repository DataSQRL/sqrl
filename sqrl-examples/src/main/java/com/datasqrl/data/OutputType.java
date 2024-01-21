package com.datasqrl.data;

public enum OutputType {

  JSON, CSV;

  public String extension() {
    return name().toLowerCase();
  }

}

/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.name;

public class ReservedName extends AbstractName {

  private final String name;

  private ReservedName(String identifier) {
    this.name = identifier;
  }

  @Override
  public String getCanonical() {
    return name;
  }

  @Override
  public String getDisplay() {
    return name;
  }

  public static final Name SELF_IDENTIFIER = Name.system("@");
  public static final ReservedName UUID = new ReservedName(HIDDEN_PREFIX + "uuid");
  public static final ReservedName INGEST_TIME = new ReservedName(HIDDEN_PREFIX + "ingest_time");
  public static final ReservedName SOURCE_TIME = new ReservedName(HIDDEN_PREFIX + "source_time");
  public static final ReservedName ARRAY_IDX = new ReservedName(HIDDEN_PREFIX + "idx");
  public static final ReservedName TIMESTAMP = new ReservedName(HIDDEN_PREFIX + "timestamp");
  public static final ReservedName PARENT = new ReservedName("parent");
  public static final ReservedName ALL = new ReservedName("*");

}

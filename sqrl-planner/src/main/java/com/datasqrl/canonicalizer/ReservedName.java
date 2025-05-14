/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.canonicalizer;

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
  public static final ReservedName SOURCE_TIME = new ReservedName(HIDDEN_PREFIX + "source_time");
  public static final ReservedName ARRAY_IDX = new ReservedName(HIDDEN_PREFIX + "idx");
  public static final ReservedName SYSTEM_TIMESTAMP = new ReservedName(SYSTEM_HIDDEN_PREFIX + "timestamp");
  public static final ReservedName SYSTEM_PRIMARY_KEY = new ReservedName(SYSTEM_HIDDEN_PREFIX + "pk");
  public static final ReservedName PARENT = new ReservedName("parent");
  public static final ReservedName ALL = new ReservedName("*");
  public static final ReservedName VARIABLE_PREFIX = new ReservedName("$");

  public static final ReservedName MUTATION_TIME = new ReservedName("event_time");
  public static final ReservedName MUTATION_PRIMARY_KEY = UUID;

}

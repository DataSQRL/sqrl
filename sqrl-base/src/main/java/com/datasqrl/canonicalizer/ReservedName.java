/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  public static final ReservedName SYSTEM_TIMESTAMP =
      new ReservedName(SYSTEM_HIDDEN_PREFIX + "timestamp");
  public static final ReservedName SYSTEM_PRIMARY_KEY =
      new ReservedName(SYSTEM_HIDDEN_PREFIX + "pk");
  public static final ReservedName PARENT = new ReservedName("parent");
  public static final ReservedName ALL = new ReservedName("*");
  public static final ReservedName VARIABLE_PREFIX = new ReservedName("$");

  public static final ReservedName MUTATION_TIME = new ReservedName("event_time");
  public static final ReservedName MUTATION_PRIMARY_KEY = UUID;
}

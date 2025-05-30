/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
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

public class SpecialName extends AbstractName {

  /* A string prefix that other names cannot have to ensure uniqueness in order
    to ensure that a special name is never confused with a "real" name.
    A space in front will ensure that.
  */
  public static final String UNIQUE_PREFIX = " #";

  private final String name;

  private SpecialName(String identifier) {
    this.name = UNIQUE_PREFIX + identifier;
  }

  @Override
  public String getCanonical() {
    return name;
  }

  @Override
  public String getDisplay() {
    return name;
  }

  public static Name SINGLETON = new SpecialName("singleton");
  public static Name LOCAL = new SpecialName("local");
  public static Name VALUE = new SpecialName("value");
}

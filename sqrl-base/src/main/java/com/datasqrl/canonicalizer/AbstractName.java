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

// import com.google.common.base.Preconditions;
// import com.google.common.base.Strings;

public abstract class AbstractName implements Name {

  public static String validateName(String name) {
    //    Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Not a valid name: %s", name);
    return name;
  }

  @Override
  public String toString() {
    return getDisplay();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    } else if (this == other) {
      return true;
    } else if (!(other instanceof Name)) {
      return false;
    }
    Name o = (Name) other;
    return getCanonical().equals(o.getCanonical());
  }

  @Override
  public int hashCode() {
    return getCanonical().hashCode();
  }

  @Override
  public int compareTo(Name o) {
    return getCanonical().compareTo(o.getCanonical());
  }

  @Override
  public boolean hasPrefix(Name variablePrefix) {
    return this.getCanonical().startsWith(variablePrefix.getCanonical());
  }
}

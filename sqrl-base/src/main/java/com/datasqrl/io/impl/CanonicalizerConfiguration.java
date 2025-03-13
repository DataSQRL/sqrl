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
package com.datasqrl.io.impl;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import java.io.Serializable;
import java.util.Locale;
import lombok.Getter;

@Getter
public enum CanonicalizerConfiguration implements Serializable {
  lowercase(NameCanonicalizer.LOWERCASE_ENGLISH),
  case_sensitive(NameCanonicalizer.AS_IS),
  system(NameCanonicalizer.SYSTEM);

  private final NameCanonicalizer canonicalizer;

  CanonicalizerConfiguration(NameCanonicalizer canonicalizer) {
    this.canonicalizer = canonicalizer;
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ENGLISH);
  }
}

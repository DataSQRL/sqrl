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
package com.datasqrl.planner.util;

import com.datasqrl.canonicalizer.Name;
import java.util.Map;
import java.util.Optional;

public interface Documented {

  Documentation EMPTY = new Documentation(null, Map.of(), Map.of());

  Lookup NO_LOOKUP = name -> Optional.empty();

  Documentation getDocumentation();

  interface Lookup {

    Optional<String> getDocsFor(String name);
  }

  record Documentation(
      String docString, Map<Name, String> columnDocs, Map<Name, String> argumentDocs) {

    public String getColumn(String columnName, String defaultValue) {
      return columnDocs.getOrDefault(Name.system(columnName), defaultValue);
    }

    public Lookup getColumnLookup() {
      return this::getColumnOpt;
    }

    public Optional<String> getColumnOpt(String columnName) {
      return Optional.ofNullable(columnDocs.get(Name.system(columnName)));
    }

    public String getArgument(String argName, String defaultValue) {
      return argumentDocs.getOrDefault(Name.system(argName), defaultValue);
    }

    public Optional<String> getArgumentOpt(String argName) {
      return Optional.ofNullable(argumentDocs.get(Name.system(argName)));
    }

    public Lookup getArgumentLookup() {
      return this::getArgumentOpt;
    }

    public boolean isEmpty() {
      return (docString == null || docString.isEmpty())
          && columnDocs().isEmpty()
          && argumentDocs().isEmpty();
    }

    public boolean hasDocString() {
      return docString != null && !docString.isEmpty();
    }

    public String getDocString(String defaultValue) {
      if (!hasDocString()) {
        return defaultValue;
      }
      return docString;
    }

    public Optional<String> getDocStringOpt() {
      if (hasDocString()) return Optional.of(docString);
      return Optional.empty();
    }
  }
}

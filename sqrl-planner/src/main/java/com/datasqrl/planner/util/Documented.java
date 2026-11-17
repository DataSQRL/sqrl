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
import com.datasqrl.canonicalizer.NamePath;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public interface Documented {

  Documentation EMPTY = new Documentation(null, Map.of(), Map.of());

  Lookup NO_LOOKUP = name -> Optional.empty();

  Documentation getDocumentation();

  /** Looks up the docs of fields by name. */
  interface Lookup {

    Optional<String> getDocsFor(String name);

    /** Returns the lookup for the fields nested in the field with the given name. */
    default Lookup nested(String name) {
      return NO_LOOKUP;
    }
  }

  /**
   * The doc string of a table or function and the docs of its columns and arguments, by path. A
   * top-level column or argument has a path of one name, and a field nested in it has the path of
   * the column followed by the name of the field.
   */
  record Documentation(
      String docString, Map<NamePath, String> columnDocs, Map<NamePath, String> argumentDocs) {

    public String getColumn(String columnName, String defaultValue) {
      return getColumnOpt(columnName).orElse(defaultValue);
    }

    public Lookup getColumnLookup() {
      return lookup(columnDocs, NamePath.ROOT);
    }

    public Optional<String> getColumnOpt(String columnName) {
      return getColumnLookup().getDocsFor(columnName);
    }

    public String getArgument(String argName, String defaultValue) {
      return getArgumentOpt(argName).orElse(defaultValue);
    }

    public Optional<String> getArgumentOpt(String argName) {
      return getArgumentLookup().getDocsFor(argName);
    }

    public Lookup getArgumentLookup() {
      return lookup(argumentDocs, NamePath.ROOT);
    }

    /**
     * Returns this documentation with the given docs for the top-level columns that it doesn't
     * document, so that its own column docs take precedence.
     *
     * @param docs the docs by column name
     */
    public Documentation withColumnDocsIfAbsent(Map<String, String> docs) {
      if (docs.isEmpty()) {
        return this;
      }
      var merged = new LinkedHashMap<>(columnDocs);
      docs.forEach((column, doc) -> merged.putIfAbsent(NamePath.of(column), doc));
      return new Documentation(docString, merged, argumentDocs);
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

    private static Lookup lookup(Map<NamePath, String> docs, NamePath parent) {
      return new Lookup() {
        @Override
        public Optional<String> getDocsFor(String name) {
          return Optional.ofNullable(docs.get(parent.concat(Name.system(name))));
        }

        @Override
        public Lookup nested(String name) {
          return lookup(docs, parent.concat(Name.system(name)));
        }
      };
    }
  }
}

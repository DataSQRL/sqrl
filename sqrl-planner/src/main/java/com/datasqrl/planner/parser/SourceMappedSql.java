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
package com.datasqrl.planner.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * SQL that is generated from literal text and fragments of a parsed statement. It keeps track of
 * where each fragment ends up in the SQL, so that positions of errors in the SQL can be mapped back
 * to the statement.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
final class SourceMappedSql {

  @Getter private final String sql;
  private final List<Fragment> fragments;

  static Builder builder() {
    return new Builder();
  }

  /**
   * Maps a location in the SQL to the corresponding location in the parsed statement.
   *
   * @return the location in the statement, or empty if the location is in literal text
   */
  Optional<FileLocation> toSourceLocation(FileLocation sqlLocation) {
    var index = toIndex(sqlLocation);
    return fragments.stream()
        .filter(fragment -> fragment.contains(index))
        .findFirst()
        .map(fragment -> fragment.toSourceLocation(index));
  }

  /** Inverse of {@link SqlScriptStatementSplitter#computeOffset(String, int)}. */
  private int toIndex(FileLocation location) {
    var lineStart = 0;
    for (var line = 1; line < location.line(); line++) {
      var lineEnd = sql.indexOf('\n', lineStart);
      if (lineEnd < 0) {
        return sql.length() + 1; // past the end of the SQL
      }
      lineStart = lineEnd + 1;
    }
    return lineStart + location.offset() - 1;
  }

  /** A fragment of the parsed statement that starts at the given index of the SQL. */
  private record Fragment(int start, ParsedObject<String> source) {

    /** Whether the index is within the fragment or directly after it. */
    boolean contains(int index) {
      return index >= start && index <= start + source.get().length();
    }

    FileLocation toSourceLocation(int index) {
      return SqrlStatementParser.relativeLocation(source, index - start);
    }
  }

  static final class Builder {

    private final StringBuilder sql = new StringBuilder();
    private final List<Fragment> fragments = new ArrayList<>();

    private Builder() {}

    /** Appends literal text that does not originate from the statement. */
    Builder append(String text) {
      sql.append(text);
      return this;
    }

    /** Appends a fragment of the statement whose positions are mapped back to the statement. */
    Builder append(ParsedObject<String> fragment) {
      fragments.add(new Fragment(sql.length(), fragment));
      sql.append(fragment.get());
      return this;
    }

    SourceMappedSql build() {
      return new SourceMappedSql(sql.toString(), List.copyOf(fragments));
    }
  }
}

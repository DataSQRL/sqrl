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
package com.datasqrl.discovery.stats;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.ToString;

@ToString
public class SourceTableStatistics
    implements Accumulator<Map<String, Object>, SourceTableStatistics, Void>,
        Metric<SourceTableStatistics> {

  final RelationStats relation;

  public SourceTableStatistics() {
    this.relation = new RelationStats();
  }

  public ErrorCollector validate(Map<String, Object> data, ErrorCollector errors) {
    RelationStats.validate(data, errors, NameCanonicalizer.SYSTEM);
    return errors;
  }

  @Override
  public void add(Map<String, Object> data, Void context) {
    // TODO: Analyze timestamps on record
    relation.add(data, NameCanonicalizer.SYSTEM);
  }

  @Override
  public void merge(SourceTableStatistics accumulator) {
    relation.merge(accumulator.relation);
  }

  public long getCount() {
    return relation.getCount();
  }

  public RelationStats getRelationStats(NamePath path) {
    var current = relation;
    for (var i = 0; i < path.size(); i++) {
      var n = path.get(i);
      var field = current.fieldStats.get(n);
      if (field == null) {
        return RelationStats.EMPTY;
      }
      Preconditions.checkNotNull(field, "Could not find nested table: %s", n);
      current =
          field.types.values().stream()
              .filter(fts -> fts.nestedRelationStats != null)
              .map(fts -> fts.nestedRelationStats)
              .reduce(
                  (a, b) -> {
                    throw new IllegalStateException("Expected single RelationStats for nested");
                  })
              .orElse(RelationStats.EMPTY);
    }
    return current;
  }
}

/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery.stats;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.*;

public class RelationStats implements
    Accumulator<Map<String, Object>, RelationStats, NameCanonicalizer> {

  public static final RelationStats EMPTY = new RelationStats(0, Collections.EMPTY_MAP);
  private static final int INITIAL_CAPACITY = 8;

  long count;
  Map<Name, FieldStats> fieldStats;

  public RelationStats() {
    this.fieldStats = new LinkedHashMap<>(INITIAL_CAPACITY);
    this.count = 0;
  }

  private RelationStats(long count, Map<Name, FieldStats> fieldStats) {
    this.count = count;
    this.fieldStats = fieldStats;
  }

  public RelationStats clone() {
    RelationStats copy = new RelationStats();
    copy.merge(this);
    return copy;
  }

  public long getCount() {
    return count;
  }

  public static void validate(Map<String, Object> value, ErrorCollector errors,
      NameCanonicalizer canonicalizer) {
    if (value == null || value.isEmpty()) {
      errors.fatal("Invalid value: %s", value);
    }
    Set<Name> names = new HashSet<>(value.size());
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      String name = entry.getKey();
      if (Strings.isNullOrEmpty(name)) {
        errors.fatal("Invalid name: %s", name);
      }
      if (!names.add(Name.of(name, canonicalizer))) {
        errors.fatal("Duplicate name: %s", name);
      }
      FieldStats.validate(entry.getValue(), errors.resolve(name), canonicalizer);
    }
  }

  void add(Name name, FieldStats field) {
    Preconditions.checkNotNull(!fieldStats.containsKey(name));
    fieldStats.put(name, field);
  }

  @Override
  public void add(Map<String, Object> value, NameCanonicalizer canonicalizer) {
    count++;
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      Name name = Name.of(entry.getKey(), canonicalizer);
      FieldStats fieldAccum = fieldStats.get(name);
      if (fieldAccum == null) {
        fieldAccum = new FieldStats();
        fieldStats.put(name, fieldAccum);
      }
      fieldAccum.add(entry.getValue(), entry.getKey(), canonicalizer);
    }
  }

  @Override
  public void merge(RelationStats acc) {
    count += acc.count;
    acc.fieldStats.forEach((k, v) -> {
      FieldStats fieldaccum = fieldStats.get(k);
      if (fieldaccum == null) {
        fieldaccum = new FieldStats();
        fieldStats.put(k, fieldaccum);
      }
      fieldaccum.merge(v);
    });
  }

}

/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery.stats;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.schema.input.RelationType;
import com.datasqrl.schema.input.TypeSignature.Simple;
import com.datasqrl.schema.input.TypeSignatureUtil;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BasicType;
import com.datasqrl.schema.type.basic.BasicTypeManager;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

public class FieldStats implements Serializable {

  long count = 0;
  long numNulls = 0;
  Map<FieldTypeStats, FieldTypeStats> types = new HashMap<>(4);
  Map<String, AtomicLong> nameCounts = new HashMap<>(2);

  public FieldStats() {}

  public static void validate(Object o, ErrorCollector errors, NameCanonicalizer canonicalizer) {
    if (TypeSignatureUtil.isArray(o)) {
      Type type = null;
      Pair<Stream<Object>, Integer> array = TypeSignatureUtil.flatMapArray(o);
      Iterator<Object> arrIter = array.getLeft().iterator();
      while (arrIter.hasNext()) {
        Object next = arrIter.next();
        if (next == null) {
          continue;
        }
        Type elementType;
        if (next instanceof Map) {
          elementType = RelationType.EMPTY;
          if (array.getRight() != 1) {
            errors.fatal("Nested arrays of objects are not supported: [%s]", o);
          }
          RelationStats.validate((Map) next, errors, canonicalizer);
        } else {
          // since we flatmapped, this must be a scalar
          elementType = TypeSignatureUtil.getBasicType(next, errors);
          if (elementType == null) {
            return;
          }
        }
        if (type == null) {
          type = elementType;
        } else if (!elementType.equals(type)) {
          if (type instanceof BasicType && elementType instanceof BasicType) {
            type = BasicTypeManager.combineForced((BasicType) type, (BasicType) elementType);
          } else {
            errors.fatal(
                "Array contains elements with incompatible types: [%s]. Found [%s] and [%s]",
                o, type, elementType);
          }
        }
      }
    } else if (o != null) {
      // Single element
      if (o instanceof Map) {
        RelationStats.validate((Map) o, errors, canonicalizer);
      } else {
        // not an array or map => must be scalar
        TypeSignatureUtil.getBasicType(o, errors);
      }
    }
  }

  public void add(Object o, @NonNull String displayName, NameCanonicalizer canonicalizer) {
    count++;
    addNameCount(displayName, 1);
    Optional<Simple> typeSignatureOpt =
        TypeSignatureUtil.detectSimpleTypeSignature(
            o, BasicTypeManager::detectType, BasicTypeManager::detectType);
    if (typeSignatureOpt.isEmpty()) {
      numNulls++;
    } else {
      Simple typeSignature = typeSignatureOpt.get();
      FieldTypeStats fieldStats = setOrGet(FieldTypeStats.of(typeSignature));
      if (TypeSignatureUtil.isArray(o)) { // Processes nested maps if any
        Iterator<Object> arrIter = TypeSignatureUtil.flatMapArray(o).getLeft().iterator();
        int numElements = 0;
        while (arrIter.hasNext()) {
          Object next = arrIter.next();
          if (next == null) {
            continue;
          }
          if (next instanceof Map) {
            fieldStats.addNested((Map) next, canonicalizer);
          }
          numElements++;
        }
        fieldStats.add(numElements);
      } else {
        fieldStats.add();
        if (o instanceof Map) {
          fieldStats.addNested((Map) o, canonicalizer);
        }
      }
    }
  }

  private FieldTypeStats setOrGet(FieldTypeStats stats) {
    FieldTypeStats existing = types.get(stats);
    if (existing != null) {
      return existing;
    }
    types.put(stats, stats);
    return stats;
  }

  public void merge(FieldStats acc) {
    count += acc.count;
    numNulls += acc.numNulls;
    for (FieldTypeStats fstats : acc.types.keySet()) {
      FieldTypeStats thisStats = types.get(fstats);
      if (thisStats == null) {
        thisStats = new FieldTypeStats(fstats);
        types.put(thisStats, thisStats);
      }
      thisStats.merge(fstats);
    }
    acc.nameCounts.forEach((n, c) -> addNameCount(n, c.get()));
  }

  private void addNameCount(@NonNull String name, long count) {
    name = name.trim();
    AtomicLong counter = nameCounts.get(name);
    if (counter == null) {
      counter = new AtomicLong(0);
      nameCounts.put(name, counter);
    }
    counter.addAndGet(count);
  }

  String getDisplayName() {
    return nameCounts.entrySet().stream()
        .max(
            new Comparator<Map.Entry<String, AtomicLong>>() {
              @Override
              public int compare(
                  Map.Entry<String, AtomicLong> o1, Map.Entry<String, AtomicLong> o2) {
                return Long.compare(o1.getValue().get(), o2.getValue().get());
              }
            })
        .get()
        .getKey();
  }
}

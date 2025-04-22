/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery.stats;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.input.RelationType;
import com.datasqrl.io.schema.flexible.input.TypeSignatureUtil;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.io.schema.flexible.type.basic.BasicType;
import com.datasqrl.io.schema.flexible.type.basic.BasicTypeManager;

import lombok.NonNull;

public class FieldStats implements Serializable {

  long count = 0;
  long numNulls = 0;
  Map<FieldTypeStats, FieldTypeStats> types = new HashMap<>(4);
  Map<String, AtomicLong> nameCounts = new HashMap<>(2);

  public FieldStats() {
  }

  public static void validate(Object o, ErrorCollector errors,
      NameCanonicalizer canonicalizer) {
    if (TypeSignatureUtil.isArray(o)) {
      Type type = null;
      var array = TypeSignatureUtil.flatMapArray(o);
      var arrIter = array.getLeft().iterator();
      while (arrIter.hasNext()) {
        var next = arrIter.next();
        if (next == null) {
          continue;
        }
        Type elementType;
        if (next instanceof Map map) {
          elementType = RelationType.EMPTY;
          if (array.getRight() != 1) {
            errors.fatal("Nested arrays of objects are not supported: [%s]", o);
          }
          RelationStats.validate(map, errors, canonicalizer);
        } else {
          //since we flatmapped, this must be a scalar
          elementType = TypeSignatureUtil.getBasicType(next, errors);
          if (elementType == null) {
            return;
          }
        }
        if (type == null) {
          type = elementType;
        } else if (!elementType.equals(type)) {
          if (type instanceof BasicType basicType && elementType instanceof BasicType basicType1) {
            type = BasicTypeManager.combineForced(basicType, basicType1);
          } else {
            errors.fatal(
                "Array contains elements with incompatible types: [%s]. Found [%s] and [%s]",
                o,
                type, elementType);
          }
        }
      }
    } else if (o != null) {
      //Single element
      if (o instanceof Map map) {
        RelationStats.validate(map, errors, canonicalizer);
      } else {
        //not an array or map => must be scalar
        TypeSignatureUtil.getBasicType(o, errors);
      }
    }
  }

  public void add(Object o, @NonNull String displayName, NameCanonicalizer canonicalizer) {
    count++;
    addNameCount(displayName, 1);
    var typeSignatureOpt = TypeSignatureUtil.detectSimpleTypeSignature(o, BasicTypeManager::detectType,
        BasicTypeManager::detectType);
    if (typeSignatureOpt.isEmpty()) {
      numNulls++;
    } else {
      var typeSignature = typeSignatureOpt.get();
      var fieldStats = setOrGet(FieldTypeStats.of(typeSignature));
      if (TypeSignatureUtil.isArray(o)) { //Processes nested maps if any
        var arrIter = TypeSignatureUtil.flatMapArray(o).getLeft().iterator();
        var numElements = 0;
        while (arrIter.hasNext()) {
          var next = arrIter.next();
          if (next == null) {
            continue;
          }
          if (next instanceof Map map) {
            fieldStats.addNested(map, canonicalizer);
          }
          numElements++;
        }
        fieldStats.add(numElements);
      } else {
        fieldStats.add();
        if (o instanceof Map map) {
          fieldStats.addNested(map, canonicalizer);
        }
      }
    }
  }

  private FieldTypeStats setOrGet(FieldTypeStats stats) {
    var existing = types.get(stats);
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
      var thisStats = types.get(fstats);
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
    var counter = nameCounts.get(name);
    if (counter == null) {
      counter = new AtomicLong(0);
      nameCounts.put(name, counter);
    }
    counter.addAndGet(count);
  }

  String getDisplayName() {
    return nameCounts.entrySet().stream().max((o1, o2) -> Long.compare(o1.getValue().get(), o2.getValue().get())).get().getKey();
  }

}

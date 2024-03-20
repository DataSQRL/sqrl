/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.metadata.stats;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.schema.input.TypeSignature;
import com.datasqrl.schema.type.Type;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import lombok.NonNull;

public class FieldTypeStats implements Serializable, Cloneable, TypeSignature {

  Type raw;
  Type detected;
  int arrayDepth;

  long count;
  //Only not-null if detected is an array
  LogarithmicHistogram.Accumulator arrayCardinality;
  //Only not-null if detected is a NestedRelation
  RelationStats nestedRelationStats;

  public FieldTypeStats() {
  } //For Kryo;

  public FieldTypeStats(@NonNull Type raw, int arrayDepth) {
    this(raw, raw, arrayDepth);
  }

  public FieldTypeStats(@NonNull Type raw, @NonNull Type detected, int arrayDepth) {
    this.raw = raw;
    this.detected = detected;
    this.arrayDepth = arrayDepth;
    this.count = 0;
  }

  public FieldTypeStats(FieldTypeStats other) {
    this(other.raw, other.detected, other.arrayDepth);
  }

  public static FieldTypeStats of(@NonNull TypeSignature.Simple signature) {
    return new FieldTypeStats(signature.getRaw(), signature.getDetected(),
        signature.getArrayDepth());
  }

  @Override
  public Type getRaw() {
    return raw;
  }

  @Override
  public Type getDetected() {
    return detected;
  }

  @Override
  public int getArrayDepth() {
    return arrayDepth;
  }

  public void add() {
    count++;
  }

  public void addNested(@NonNull Map<String, Object> nested,
      @NonNull NameCanonicalizer canonicalizer) {
    if (nestedRelationStats == null) {
      nestedRelationStats = new RelationStats();
    }
    nestedRelationStats.add(nested, canonicalizer);
  }

  public void add(int numArrayElements) {
    count++;
    addArrayCardinality(numArrayElements);
  }

  public static final int ARRAY_CARDINALITY_BASE = 4;
  public static final int ARRAY_CARDINALITY_BUCKETS = 8;

  private void addArrayCardinality(int numElements) {
    if (arrayCardinality == null) {
      arrayCardinality = new LogarithmicHistogram.Accumulator(ARRAY_CARDINALITY_BASE,
          ARRAY_CARDINALITY_BUCKETS);
    }
    arrayCardinality.add(numElements);
  }

  public void merge(@NonNull FieldTypeStats other) {
    assert equals(other);
    count += other.count;
    if (other.arrayCardinality != null) {
      if (arrayCardinality == null) {
        arrayCardinality = new LogarithmicHistogram.Accumulator(ARRAY_CARDINALITY_BASE,
            ARRAY_CARDINALITY_BUCKETS);
      }
      arrayCardinality.merge(other.arrayCardinality);
    }
    if (other.nestedRelationStats != null) {
      if (nestedRelationStats == null) {
        nestedRelationStats = new RelationStats();
      }
      nestedRelationStats.merge(other.nestedRelationStats);
    }
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldTypeStats that = (FieldTypeStats) o;
    return raw.equals(that.raw) && detected.equals(that.detected) && arrayDepth == that.arrayDepth;
  }

  @Override
  public int hashCode() {
    return Objects.hash(raw, detected, arrayDepth);
  }

  @Override
  public String toString() {
    String result = "{" + raw.toString();
    if (!raw.equals(detected)) {
      result += "|" + detected.toString();
    }
    result += "}^" + arrayDepth;
    return result;
  }


}



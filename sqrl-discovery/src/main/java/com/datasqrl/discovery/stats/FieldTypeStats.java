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
import com.datasqrl.io.schema.flexible.input.TypeSignature;
import com.datasqrl.io.schema.flexible.type.Type;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import lombok.NonNull;

public class FieldTypeStats implements Serializable, Cloneable, TypeSignature {

  Type raw;
  Type detected;
  int arrayDepth;

  long count;
  // Only not-null if detected is an array
  LogarithmicHistogram.Accumulator arrayCardinality;
  // Only not-null if detected is a NestedRelation
  RelationStats nestedRelationStats;

  public FieldTypeStats() {} // For Kryo;

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
    return new FieldTypeStats(signature.raw(), signature.detected(), signature.arrayDepth());
  }

  @Override
  public Type raw() {
    return raw;
  }

  @Override
  public Type detected() {
    return detected;
  }

  @Override
  public int arrayDepth() {
    return arrayDepth;
  }

  public void add() {
    count++;
  }

  public void addNested(
      @NonNull Map<String, Object> nested, @NonNull NameCanonicalizer canonicalizer) {
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
      arrayCardinality =
          new LogarithmicHistogram.Accumulator(ARRAY_CARDINALITY_BASE, ARRAY_CARDINALITY_BUCKETS);
    }
    arrayCardinality.add(numElements);
  }

  public void merge(@NonNull FieldTypeStats other) {
    assert equals(other);
    count += other.count;
    if (other.arrayCardinality != null) {
      if (arrayCardinality == null) {
        arrayCardinality =
            new LogarithmicHistogram.Accumulator(ARRAY_CARDINALITY_BASE, ARRAY_CARDINALITY_BUCKETS);
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
    var that = (FieldTypeStats) o;
    return raw.equals(that.raw) && detected.equals(that.detected) && arrayDepth == that.arrayDepth;
  }

  @Override
  public int hashCode() {
    return Objects.hash(raw, detected, arrayDepth);
  }

  @Override
  public String toString() {
    var result = "{" + raw.toString();
    if (!raw.equals(detected)) {
      result += "|" + detected.toString();
    }
    result += "}^" + arrayDepth;
    return result;
  }
}

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

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Arrays;
import lombok.ToString;
import lombok.Value;

@Value
public class LogarithmicHistogram implements Serializable {

  public static final LogarithmicHistogram EMPTY = new LogarithmicHistogram(2, 0, new long[0]);

  private float base;
  private long count;
  private long[] buckets;
  private long numZeros;

  public LogarithmicHistogram(float base, long count, long[] buckets) {
    this.base = base;
    this.count = count;
    this.buckets = buckets;
    this.numZeros = count - Arrays.stream(buckets).sum();
  }

  public float getBase() {
    return base;
  }

  public long getCount() {
    return count;
  }

  @ToString
  public static class Accumulator
      implements com.datasqrl.discovery.stats.Accumulator<Long, Accumulator, Void> {

    private float base;
    private double baseConversion;
    private long[] buckets;
    private long count;

    private Accumulator() {
      // For Kryo
    }

    public Accumulator(float base, int maxBuckets) {
      Preconditions.checkArgument(base > 1 && base < 100, "Invalid base provided: %s", base);
      Preconditions.checkArgument(
          maxBuckets > 0 && maxBuckets < 1000, "Invalid number of buckets: %s", maxBuckets);
      this.buckets = new long[maxBuckets];
      this.count = 0;
      this.base = (byte) base;
      this.baseConversion = Math.log(base);
    }

    @Override
    public void add(Long value, Void v) {
      add(value);
    }

    public void add(long value) {
      Preconditions.checkArgument(value >= 0, "Value must be positive: %s", value);
      count++;
      if (value > 0) {
        var index = (int) Math.ceil(Math.log(value) / baseConversion);
        if (index >= buckets.length) {
          index = buckets.length - 1;
        }
        buckets[index]++;
      }
    }

    public LogarithmicHistogram getLocalValue() {
      var b = Arrays.copyOf(buckets, buckets.length);
      return new LogarithmicHistogram(base, count, b);
    }

    public void resetLocal() {
      for (var i = 0; i < buckets.length; i++) {
        buckets[i] = 0;
      }
      count = 0;
    }

    @Override
    public void merge(LogarithmicHistogram.Accumulator accumulator) {
      var acc = accumulator;
      Preconditions.checkArgument(base == acc.base, "Incompatible bases: %s vs %s", base, acc.base);
      Preconditions.checkArgument(
          buckets.length == acc.buckets.length, "Incompatible histogram widths");
      count += acc.count;
      for (var i = 0; i < buckets.length; i++) {
        buckets[i] += acc.buckets[i];
      }
    }

    @Override
    public LogarithmicHistogram.Accumulator clone() {
      var newAcc = new Accumulator(base, buckets.length);
      newAcc.count = count;
      for (var i = 0; i < buckets.length; i++) {
        newAcc.buckets[i] = buckets[i];
      }
      return newAcc;
    }
  }
}

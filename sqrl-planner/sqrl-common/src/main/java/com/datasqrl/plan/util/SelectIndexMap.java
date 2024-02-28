/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.util;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.ArrayUtils;

@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class SelectIndexMap implements IndexMap, Serializable {

  public static final SelectIndexMap EMPTY = new SelectIndexMap(new int[0]);

  final int[] targets;

  @Override
  public int mapUnsafe(int index) {
    if (index<0 || index>=targets.length) return -1;
    return targets[index];
  }

  public int getSourceLength() {
    return targets.length;
  }

  public int[] targetsAsArray() {
    return targets.clone();
  }

  public List<Integer> targetsAsList() {
    return Ints.asList(targetsAsArray());
  }

  public SelectIndexMap join(SelectIndexMap right, int leftSideWidth, boolean isFlipped) {
    int[] combined = new int[targets.length + right.targets.length];
    //Left map doesn't change
    int offset=0;
    int[][] arrsToCopy = isFlipped?new int[][]{right.targets,targets}:new int[][]{targets, right.targets};
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < arrsToCopy[i].length; j++) {
        combined[offset + j] = ((isFlipped ^ i==1)?leftSideWidth:0) + arrsToCopy[i][j];
      }
      offset += arrsToCopy[i].length;
    }
    return new SelectIndexMap(combined);
  }

  public boolean isIdentity() {
    Preconditions.checkArgument(targets.length>0);
    return IntStream.range(0,targets.length).allMatch(i -> targets[i]==i);
  }

  public SelectIndexMap append(SelectIndexMap add) {
    int[] combined = new int[targets.length + add.targets.length];
    System.arraycopy(targets, 0, combined, 0, targets.length);
    System.arraycopy(add.targets, 0, combined, targets.length, add.targets.length);
    return new SelectIndexMap(combined);
  }

  public SelectIndexMap add(int index) {
    int[] newTargets = Arrays.copyOf(targets, targets.length+1);
    newTargets[targets.length] = index;
    return new SelectIndexMap(newTargets);
  }

  public SelectIndexMap remap(IndexMap remap) {
    Builder b = new Builder(targets.length);
    for (int i = 0; i < targets.length; i++) {
      b.add(remap.map(map(i)));
    }
    return b.build();
  }

  public static Builder builder(int length) {
    return new Builder(length);
  }

  public static Builder builder(SelectIndexMap base, int addedLength) {
    Builder b = new Builder(base.getSourceLength() + addedLength);
    return b.addAll(base);
  }

  public static SelectIndexMap identity(int sourceLength, int targetLength) {
    Builder b = builder(sourceLength);
    for (int i = 0; i < sourceLength; i++) {
      b.add(i);
    }
    return b.build(targetLength);
  }

  public static final class Builder {

    final int[] map;
    int offset = 0;

    private Builder(int length) {
      this.map = new int[length];
    }

    public int remaining() {
      return map.length-offset;
    }

    public Builder addAll(SelectIndexMap indexMap) {
      for (int i = 0; i < indexMap.targets.length; i++) {
        add(indexMap.targets[i]);
      }
      return this;
    }

    public Builder addAll(Iterable<Integer> mapTo) {
      mapTo.forEach(i -> add(i));
      return this;
    }

    public Builder add(int mapTo) {
      Preconditions.checkArgument(offset < map.length);
      map[offset] = mapTo;
      offset++;
      return this;
    }

    public SelectIndexMap build(int targetLength) {
      Preconditions.checkArgument(Arrays.stream(map).noneMatch(i -> i >= targetLength));
      return build();
    }

    public SelectIndexMap build() {
      Preconditions.checkArgument(offset == map.length);
      return new SelectIndexMap(map);
    }


  }
}

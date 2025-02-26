package com.datasqrl.plan.table;

import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.util.ArrayUtil;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Value;

@Value
public class PrimaryKey {

  int[] pkIndexes;

  public boolean isDefined() {
    return pkIndexes != null;
  }

  public boolean isUndefined() {
    return !isDefined();
  }

  public int get(int index) {
    Preconditions.checkArgument(isDefined());
    Preconditions.checkArgument(index >= 0 && index < pkIndexes.length);
    return pkIndexes[index];
  }

  public int size() {
    Preconditions.checkArgument(isDefined());
    return pkIndexes.length;
  }

  public boolean contains(int index) {
    Preconditions.checkArgument(isDefined());
    return ArrayUtil.contains(pkIndexes, index);
  }

  public int[] asArray() {
    Preconditions.checkArgument(isDefined());
    return pkIndexes.clone();
  }

  public List<Integer> asList() {
    return IntStream.of(pkIndexes).boxed().collect(Collectors.toUnmodifiableList());
  }

  public static PrimaryKey of(PrimaryKeyMap pkMap) {
    if (pkMap.isUndefined()) return new PrimaryKey(null);
    // Post-processing ensures the pk is simple
    int[] pkCols = ArrayUtil.toArray(pkMap.asSimpleList());
    return new PrimaryKey(pkCols);
  }

  public PrimaryKeyMap toKeyMap() {
    if (isUndefined()) return PrimaryKeyMap.UNDEFINED;
    else return PrimaryKeyMap.of(asList());
  }
}

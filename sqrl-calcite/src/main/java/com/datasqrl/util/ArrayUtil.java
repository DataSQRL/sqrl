/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import java.util.List;

import com.google.common.base.Preconditions;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ArrayUtil {

  public static boolean contains(int[] arr, int value, int endIndex) {
    Preconditions.checkArgument(endIndex>=0 && endIndex<=arr.length);
    for (var i = 0; i < endIndex; i++) {
      if (arr[i] == value) {
        return true;
      }
    }
    return false;
  }

  public static boolean contains(int[] arr, int value) {
    return contains(arr, value, arr.length);
  }

  public static int[] toArray(List<Integer> list) {
    return list.stream().mapToInt(i->i).toArray();
  }


}

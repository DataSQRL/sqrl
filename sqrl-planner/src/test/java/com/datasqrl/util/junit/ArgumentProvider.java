/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.junit;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import com.google.common.base.Preconditions;

public class ArgumentProvider {

  public static Stream<? extends Arguments> crossProduct(List<? extends Object>... argumentLists) {
    return crossProduct(List.of(argumentLists));
  }

  public static Stream<? extends Arguments> crossProduct(
      List<List<? extends Object>> argumentLists) {
    Preconditions.checkArgument(argumentLists.size() > 0);
    Stream<? extends Arguments> base = argumentLists.get(0).stream().map(Arguments::of);
    for (var i = 1; i < argumentLists.size(); i++) {
      List<? extends Object> join = argumentLists.get(i);
      base = base.flatMap(args -> join.stream().map(x -> {
        var argArr = args.get();
        var newArgArr = Arrays.copyOf(argArr, argArr.length + 1);
        newArgArr[argArr.length] = x;
        return Arguments.of(newArgArr);
      }));
    }
    return base;
  }

  public static Stream<? extends Arguments> of(List<? extends Object> arguments) {
    return arguments.stream().map(Arguments::of);
  }

}

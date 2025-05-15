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
package com.datasqrl.util.junit;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

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
      base =
          base.flatMap(
              args ->
                  join.stream()
                      .map(
                          x -> {
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

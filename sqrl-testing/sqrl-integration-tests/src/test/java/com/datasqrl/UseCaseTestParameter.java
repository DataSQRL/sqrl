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
package com.datasqrl;

import java.nio.file.Path;
import java.util.Comparator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class UseCaseTestParameter implements Comparable<UseCaseTestParameter> {

  Path rootDir;
  String goal;

  @ToString.Include(rank = 0)
  String useCaseName;

  @ToString.Include(rank = 1)
  String sqrlFileName;

  String graphqlFileName;
  String testName;
  String testPath;
  String optionalParam; // Can be null
  String packageJsonPath; // Can be null

  public UseCaseTestParameter cloneWithGoal(String goal) {
    return new UseCaseTestParameter(
        rootDir,
        goal,
        useCaseName,
        sqrlFileName,
        graphqlFileName,
        testName,
        testPath,
        optionalParam,
        packageJsonPath);
  }

  private static final Comparator<UseCaseTestParameter> ORDER =
      Comparator.comparing(UseCaseTestParameter::getSqrlFileName)
          .thenComparing(UseCaseTestParameter::getGoal)
          .thenComparing(UseCaseTestParameter::toString);

  @Override
  public int compareTo(UseCaseTestParameter o) {
    return ORDER.compare(this, o);
  }
}

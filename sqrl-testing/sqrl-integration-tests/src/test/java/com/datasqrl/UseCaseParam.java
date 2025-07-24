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

public record UseCaseParam(Path packageJsonPath, String goal) implements Comparable<UseCaseParam> {

  public String getPackageJsonName() {
    return packageJsonPath.getFileName().toString();
  }

  public String getUseCaseName() {
    return packageJsonPath.getParent().getFileName().toString();
  }

  @Override
  public int compareTo(UseCaseParam other) {
    // Compare by Path first
    int cmp = this.packageJsonPath.compareTo(other.packageJsonPath);
    if (cmp != 0) return cmp;

    // If Paths are equal, compare by goal
    return this.goal.compareTo(other.goal);
  }
}

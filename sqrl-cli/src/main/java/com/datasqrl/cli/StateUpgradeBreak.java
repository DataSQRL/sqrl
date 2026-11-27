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
package com.datasqrl.cli;

/**
 * One reason why Flink cannot restore the state of an old build from a new build.
 *
 * @param message the break as one line of text, for example {@code operator id=4 removed:
 *     Deduplicate(...)}
 * @param oldState what happens to the old state of the operator of this break
 */
record StateUpgradeBreak(String message, OldState oldState) {

  /**
   * What happens to the old state of the operator of a break. Flink gives the old state of an
   * operator to the operator of the new plan with the same uid. The default uid is {@code
   * <id>_<transformation>}: the id and the kind of the operator, without its version.
   */
  enum OldState {
    /**
     * No operator of the new plan claims the old state. Flink restores without that state only when
     * {@code execution.state-recovery.ignore-unclaimed-state} is true, and it then drops that
     * state.
     */
    UNCLAIMED,
    /**
     * An operator of the new plan claims the old state but cannot read it. The option {@code
     * execution.state-recovery.ignore-unclaimed-state} does not help.
     */
    INCOMPATIBLE,
    /**
     * The old operator has no state, for example a {@code calc} or an {@code exchange}. A {@code
     * pipeline.max-parallelism} break also has this value, because it applies to the state of all
     * operators, not to the state of one operator.
     */
    NONE
  }
}

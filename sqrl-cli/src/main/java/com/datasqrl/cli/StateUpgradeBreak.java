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

/** One reason why Flink cannot restore the state of an old build from a new build. */
public sealed interface StateUpgradeBreak
    permits StateUpgradeBreak.RemovedOperator,
        StateUpgradeBreak.ChangedOperatorType,
        StateUpgradeBreak.ChangedStateField,
        StateUpgradeBreak.ChangedMaxParallelism {

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
    /** The old operator has no state, for example a {@code calc} or an {@code exchange}. */
    NONE
  }

  /** The break as one line of text. */
  String toMessage();

  /** Returns what happens to the old state of the operator of this break. */
  OldState oldState();

  /**
   * The new plan has no operator with this id. When the old operator can hold state, no operator
   * claims that state.
   */
  record RemovedOperator(String operatorId, String description, OldState oldState)
      implements StateUpgradeBreak {

    @Override
    public String toMessage() {
      return "operator id=%s removed: %s".formatted(operatorId, description);
    }
  }

  /**
   * The new plan has an operator with this id, but a different {@code type}, for example a new
   * ExecNode version. When the old operator can hold state and the kind of the operator changes,
   * for example from {@code stream-exec-deduplicate_1} to {@code stream-exec-calc_1}, no operator
   * claims that state. A new version of the same kind, for example {@code stream-exec-sink_2},
   * keeps the uid. The description is the one of the old operator.
   */
  record ChangedOperatorType(
      String operatorId, String oldType, String newType, String description, OldState oldState)
      implements StateUpgradeBreak {

    @Override
    public String toMessage() {
      return "operator id=%s changed: field 'type' differs: %s -> %s (%s)"
          .formatted(operatorId, oldType, newType, description);
    }
  }

  /**
   * The new plan has an operator with this id, but one of its state fields is different. The values
   * are compact JSON, or {@code missing} for a missing field. The description is the one of the old
   * operator.
   */
  record ChangedStateField(
      String operatorId,
      String fieldName,
      String oldValue,
      String newValue,
      String description,
      OldState oldState)
      implements StateUpgradeBreak {

    @Override
    public String toMessage() {
      return "operator id=%s changed: field '%s' differs: %s -> %s (%s)"
          .formatted(operatorId, fieldName, oldValue, newValue, description);
    }
  }

  /**
   * The two builds set different values for {@code pipeline.max-parallelism}. Flink divides the
   * state into this number of key groups, so a restore needs the same value.
   */
  record ChangedMaxParallelism(int oldMaxParallelism, int newMaxParallelism)
      implements StateUpgradeBreak {

    /** The value applies to the state of all operators, not to the state of one operator. */
    @Override
    public OldState oldState() {
      return OldState.NONE;
    }

    @Override
    public String toMessage() {
      return "pipeline.max-parallelism differs: %d -> %d"
          .formatted(oldMaxParallelism, newMaxParallelism);
    }
  }
}

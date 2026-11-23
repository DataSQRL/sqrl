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
package com.datasqrl.deployment.model;

/** One reason why Flink cannot restore the state of an old build from a new build. */
public sealed interface StateUpgradeBreak
    permits StateUpgradeBreak.RemovedOperator,
        StateUpgradeBreak.ChangedOperatorType,
        StateUpgradeBreak.ChangedStateField,
        StateUpgradeBreak.ChangedMaxParallelism {

  /** The break as one line of text. */
  String toMessage();

  /**
   * The new plan has no operator with this id. The restore fails, unless Flink is configured to
   * drop the state that no operator claims.
   */
  record RemovedOperator(String operatorId, String description) implements StateUpgradeBreak {

    @Override
    public String toMessage() {
      return "operator id=%s removed: %s".formatted(operatorId, description);
    }
  }

  /**
   * The new plan has an operator with this id, but a different {@code type}, for example a new
   * ExecNode version. The description is the one of the old operator.
   */
  record ChangedOperatorType(String operatorId, String oldType, String newType, String description)
      implements StateUpgradeBreak {

    @Override
    public String toMessage() {
      return "operator id=%s changed: field 'type' differs: %s -> %s (%s)"
          .formatted(operatorId, oldType, newType, description);
    }
  }

  /**
   * The new plan has an operator with this id, but one of its state fields is different. The
   * message shows the descriptions, because the field values are large JSON blocks.
   */
  record ChangedStateField(
      String operatorId, String fieldName, String oldDescription, String newDescription)
      implements StateUpgradeBreak {

    @Override
    public String toMessage() {
      return "operator id=%s changed: field '%s' differs (old: %s, new: %s)"
          .formatted(operatorId, fieldName, oldDescription, newDescription);
    }
  }

  /**
   * The two builds set different values for {@code pipeline.max-parallelism}. Flink divides the
   * state into this number of key groups, so a restore needs the same value.
   */
  record ChangedMaxParallelism(int oldMaxParallelism, int newMaxParallelism)
      implements StateUpgradeBreak {

    @Override
    public String toMessage() {
      return "pipeline.max-parallelism differs: %d -> %d"
          .formatted(oldMaxParallelism, newMaxParallelism);
    }
  }
}

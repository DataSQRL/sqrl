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

/** One reason why a new compiled Flink plan cannot restore the state of an old one. */
public sealed interface StateUpgradeBreak
    permits StateUpgradeBreak.RemovedOperator,
        StateUpgradeBreak.ChangedOperatorType,
        StateUpgradeBreak.ChangedStateField {

  /** The {@code id} of the operator in the old plan. */
  String operatorId();

  /** The break as one line of text. */
  String toMessage();

  /**
   * The new plan has no operator with this id. The restore fails, unless Flink is configured to
   * drop the state that no operator claims.
   */
  record RemovedOperator(String operatorId, String description) implements StateUpgradeBreak {

    @Override
    public String toMessage() {
      return "operator %s removed: %s".formatted(operatorId, description);
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
      return "operator %s changed: field 'type' differs: %s -> %s (%s)"
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
      return "operator %s changed: field '%s' differs (old: %s, new: %s)"
          .formatted(operatorId, fieldName, oldDescription, newDescription);
    }
  }
}

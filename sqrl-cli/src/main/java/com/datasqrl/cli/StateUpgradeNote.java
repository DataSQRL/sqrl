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
 * A message of the state upgrade check that does not fail the compile. Each note sets its own
 * {@link Level}, because the check knows what the note means. A {@link StateUpgradeBreak} fails the
 * compile instead.
 */
public sealed interface StateUpgradeNote
    permits StateUpgradeNote.ChangedFlinkVersion,
        StateUpgradeNote.ChangedStateTtl,
        StateUpgradeNote.MaxParallelismOnlyInOldBuild,
        StateUpgradeNote.MaxParallelismOnlyInNewBuild {

  /** The level of a note. The names are the same as in {@code ErrorMessage.Severity}. */
  enum Level {
    NOTICE,
    WARN
  }

  Level level();

  /** The note as one line of text. */
  String toMessage();

  /**
   * Both plans have a {@code flinkVersion}, and the versions are different. This is information
   * only: a Flink upgrade that changes an operator also changes its {@code type}, which the check
   * reports as a break.
   */
  record ChangedFlinkVersion(String oldFlinkVersion, String newFlinkVersion)
      implements StateUpgradeNote {

    @Override
    public Level level() {
      return Level.NOTICE;
    }

    @Override
    public String toMessage() {
      return "The Flink version changes: %s -> %s".formatted(oldFlinkVersion, newFlinkVersion);
    }
  }

  /**
   * The {@code ttl} is the only change of a state entry. Flink restores the state after a TTL
   * change, so the check does not report it as a break. The level is a notice, because a warning
   * makes the agent change a good TTL back. The description is the one of the old operator.
   */
  record ChangedStateTtl(
      String operatorId, String stateName, String oldTtl, String newTtl, String description)
      implements StateUpgradeNote {

    @Override
    public Level level() {
      return Level.NOTICE;
    }

    @Override
    public String toMessage() {
      return ("operator id=%s: the TTL of state '%s' changes: %s -> %s (%s). The check does not"
              + " report a TTL change as a break.")
          .formatted(operatorId, stateName, oldTtl, newTtl, description);
    }
  }

  /**
   * Only the old build sets {@code pipeline.max-parallelism}. Flink takes the value from the
   * savepoint, so the restore fails only when the new job gets a different value from outside the
   * build.
   */
  record MaxParallelismOnlyInOldBuild(int oldMaxParallelism) implements StateUpgradeNote {

    @Override
    public Level level() {
      return Level.WARN;
    }

    @Override
    public String toMessage() {
      return ("Only the old build sets pipeline.max-parallelism (%d). Flink takes this value from the"
              + " savepoint. The restore fails only when the new job gets a different value from"
              + " outside the build, for example from the Flink cluster configuration.")
          .formatted(oldMaxParallelism);
    }
  }

  /**
   * Only the new build sets {@code pipeline.max-parallelism}. The check cannot see the value of the
   * running job.
   */
  record MaxParallelismOnlyInNewBuild(int newMaxParallelism) implements StateUpgradeNote {

    @Override
    public Level level() {
      return Level.WARN;
    }

    @Override
    public String toMessage() {
      return ("Only the new build sets pipeline.max-parallelism (%d). Make sure that the running job"
              + " uses the same value, because a different value makes the restore fail.")
          .formatted(newMaxParallelism);
    }
  }
}

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
package com.datasqrl.planner;

public enum PredicatePushdownRules {

  /** Use the default Flink stream program set and rules. */
  DEFAULT,

  /** Strip all table source related predicate pushdown rules from any stream program. */
  LIMITED_TABLE_SOURCE_RULES,

  /**
   * All {@code LIMITED_TABLE_SOURCE_RULES} changes, and also strip some additional filter rules
   * from the LOGICAL program.
   */
  LIMITED_RULES
}

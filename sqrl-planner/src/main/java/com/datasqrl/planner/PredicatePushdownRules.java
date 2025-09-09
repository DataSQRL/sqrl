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

  /** Use the default Flink stream program. */
  DEFAULT,

  /** Strip table scan related rules from the PREDICATE_PUSHDOWN program. */
  LIMITED_PP_RULES,

  /**
   * Completely omit the PREDICATE_PUSHDOWN program, and also strip filter rules from the LOGICAL
   * program.
   */
  LIMITED_FILTER_RULES
}

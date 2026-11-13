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
package com.datasqrl.loaders;

import com.datasqrl.loaders.IncludeDeployment.ScriptPlanning;
import java.util.function.Supplier;

/**
 * A script of a standalone included project.
 *
 * @param include the namespace of the standalone include
 * @param planning how the include's own deployment plans the script, determined on first use
 */
public record IncludedScript(String include, Supplier<ScriptPlanning> planning) {

  /** Whether the include's deployment runs the statements of the script. */
  public boolean runsStatements() {
    return planning.get().runsStatements();
  }

  /** Whether the include's deployment exposes the subscriptions of the script. */
  public boolean exposesSubscriptions() {
    return planning.get().exposesSubscriptions();
  }
}

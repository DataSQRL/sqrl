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
package com.datasqrl.util;

import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;

public class CalciteHacks {

  /**
   * Happens if some flink code is called and the metadata provider gets reset to flink's provider.
   */
  public static void resetToSqrlMetadataProvider() {
    // Reset sqrl metadata provider defaults
    RelMetadataQueryBase.THREAD_PROVIDERS.set(
        JaninoRelMetadataProvider.of(SqrlRelMetadataProvider.INSTANCE));
  }
}

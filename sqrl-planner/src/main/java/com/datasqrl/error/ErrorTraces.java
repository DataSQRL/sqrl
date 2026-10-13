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
package com.datasqrl.error;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Handles stack traces of user errors: they are always written to the log file, and printed to the
 * console only in debug mode ({@code SQRL_DEBUG} environment variable or {@code sqrl.debug} system
 * property), so they don't bury the actual error message.
 */
@Slf4j(topic = ErrorTraces.LOGGER)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorTraces {

  /** Logger that is routed to the log file only. */
  public static final String LOGGER = "com.datasqrl.errortrace";

  public static final String DEBUG_ENV = "SQRL_DEBUG";
  public static final String DEBUG_PROPERTY = "sqrl.debug";

  public static boolean isDebug() {
    var env = System.getenv(DEBUG_ENV);
    return (env != null && !env.isBlank()) || Boolean.getBoolean(DEBUG_PROPERTY);
  }

  public static void report(Throwable e) {
    log.info("Stack trace of error: {}", e.getMessage(), e);
    if (isDebug()) {
      e.printStackTrace();
    }
  }
}

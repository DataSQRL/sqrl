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
package com.datasqrl.config;

public enum EngineType {
  PROCESS,
  DATABASE,
  SERVER,
  LOG,
  QUERY,
  EXPORT;

  public boolean isWrite() {
    return this == PROCESS;
  }

  public boolean isRead() {
    return this == DATABASE || this == SERVER;
  }

  public boolean supportsExport() {
    return isDataStore() || this == EXPORT;
  }

  public boolean isDataStore() {
    return this == DATABASE || this == LOG;
  }

  public boolean isCompute() {
    return isWrite() || isRead();
  }
}

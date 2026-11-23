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
package com.datasqrl.planner.contract.odcs;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * The ODCS version that the compiler reads and writes. Older v3 versions write timestamp, time and
 * map types differently, so a contract of such a version has to be converted first.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OdcsVersion {

  /** The {@code apiVersion} of the contracts that the compiler writes */
  public static final String API_VERSION = "v3.2.0";

  private static final String SUPPORTED_PREFIX = "v3.2.";

  /**
   * @param apiVersion the {@code apiVersion} of a contract, such as {@code v3.2.0}
   * @throws IllegalArgumentException if the version is not supported; the message tells the user
   *     what to do
   */
  public static void check(String apiVersion) {
    if (apiVersion == null || apiVersion.isBlank()) {
      throw new IllegalArgumentException(
          "The contract has no apiVersion: set it to %s".formatted(API_VERSION));
    }
    if (!apiVersion.trim().startsWith(SUPPORTED_PREFIX)) {
      throw new IllegalArgumentException(
          "apiVersion [%s] is not supported: convert the contract to ODCS %s"
              .formatted(apiVersion.trim(), API_VERSION));
    }
  }
}

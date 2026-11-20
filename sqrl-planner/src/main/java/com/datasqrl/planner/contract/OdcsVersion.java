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
package com.datasqrl.planner.contract;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * The ODCS versions that differ in their logical types. A contract is generated for the version of
 * the provided contract that it is compared with, so that the logical types can be compared as they
 * are.
 */
@Getter
@RequiredArgsConstructor
public enum OdcsVersion {
  /** v3.0.x: no {@code timestamp} and {@code time} types, no {@code map} type */
  V3_0("v3.0.2"),
  /** v3.1.x: adds the {@code timestamp} and {@code time} types */
  V3_1("v3.1.0"),
  /** v3.2.x: adds the {@code map} type */
  V3_2("v3.2.0");

  /** The latest {@code apiVersion} of this version */
  private final String apiVersion;

  public static final OdcsVersion LATEST = V3_2;

  /** Whether the version has the {@code timestamp} and {@code time} logical types. */
  public boolean hasTimestamp() {
    return this != V3_0;
  }

  /** Whether the version has the {@code map} logical type. */
  public boolean hasMap() {
    return this == V3_2;
  }

  /**
   * @param apiVersion the {@code apiVersion} of a contract, such as {@code v3.1.0}
   * @throws IllegalArgumentException if the version is not a supported v3 version; the message
   *     tells the user what to do
   */
  public static OdcsVersion parse(String apiVersion) {
    if (apiVersion == null || apiVersion.isBlank()) {
      throw new IllegalArgumentException("The contract has no apiVersion: set it to v3.2.0");
    }

    var version = apiVersion.trim();
    if (version.startsWith("v3.0.")) {
      return V3_0;
    }
    if (version.startsWith("v3.1.")) {
      return V3_1;
    }
    if (version.startsWith("v3.2.")) {
      return V3_2;
    }
    if (version.startsWith("v2.")) {
      throw new IllegalArgumentException(
          "apiVersion [%s] is not supported: convert the contract to ODCS v3".formatted(version));
    }
    throw new IllegalArgumentException(
        "apiVersion [%s] is not supported: supported versions are v3.0, v3.1 and v3.2"
            .formatted(version));
  }
}

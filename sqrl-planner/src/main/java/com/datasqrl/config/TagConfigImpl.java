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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Value;

/** A tag definition under {@code compiler.tags}, mapped from the package configuration. */
@Value
@JsonIgnoreProperties(ignoreUnknown = true)
public class TagConfigImpl implements PackageJson.TagConfig {

  String name;
  List<String> values;
  List<PackageJson.RequiredColumnConfig> requiredColumns;

  @JsonCreator
  public TagConfigImpl(
      @JsonProperty("name") String name,
      @JsonProperty("values") List<String> values,
      @JsonProperty("required-columns") List<RequiredColumnConfigImpl> requiredColumns) {
    this.name = name;
    this.values = values == null ? List.of() : List.copyOf(values);
    this.requiredColumns =
        requiredColumns == null
            ? List.of()
            : List.<PackageJson.RequiredColumnConfig>copyOf(requiredColumns);
  }

  @Value
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RequiredColumnConfigImpl implements PackageJson.RequiredColumnConfig {

    String name;
    String type;

    @JsonCreator
    public RequiredColumnConfigImpl(
        @JsonProperty("name") String name, @JsonProperty("type") String type) {
      this.name = name;
      this.type = type;
    }
  }
}

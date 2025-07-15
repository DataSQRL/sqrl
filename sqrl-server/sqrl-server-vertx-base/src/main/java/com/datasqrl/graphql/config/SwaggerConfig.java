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
package com.datasqrl.graphql.config;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
@AllArgsConstructor
public class SwaggerConfig {

  public SwaggerConfig() {}

  public SwaggerConfig(JsonObject json) {
    SwaggerConfigOptionsConverter.fromJson(json, this);
  }

  @Default boolean enabled = true;
  @Default String endpoint = "/swagger";
  @Default String uiEndpoint = "/swagger-ui";
  @Default String title = "DataSQRL REST API";
  @Default String description = "Auto-generated REST API documentation for DataSQRL endpoints";
  @Default String version = "1.0.0";
  @Default String contact = "DataSQRL";
  @Default String contactUrl = "https://datasqrl.com";
  @Default String contactEmail = "contact@datasqrl.com";
  @Default String license = "Apache License 2.0";
  @Default String licenseUrl = "https://www.apache.org/licenses/LICENSE-2.0";
}

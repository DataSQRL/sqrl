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
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class SwaggerConfig {

  public SwaggerConfig() {
    this.enabled = true;
    this.endpoint = "/swagger";
    this.uiEndpoint = "/swagger-ui";
    this.title = "DataSQRL REST API";
    this.description = "Auto-generated REST API documentation for DataSQRL endpoints";
    this.version = "1.0.0";
    this.contact = "DataSQRL";
    this.contactUrl = "https://datasqrl.com";
    this.contactEmail = "contact@datasqrl.com";
    this.license = "Apache License 2.0";
    this.licenseUrl = "https://www.apache.org/licenses/LICENSE-2.0";
  }

  public SwaggerConfig(JsonObject json) {
    this(); // Initialize with defaults
    SwaggerConfigOptionsConverter.fromJson(json, this);
  }

  boolean enabled;
  String endpoint;
  String uiEndpoint;
  String title;
  String description;
  String version;
  String contact;
  String contactUrl;
  String contactEmail;
  String license;
  String licenseUrl;
}

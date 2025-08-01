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
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SwaggerConfig {

  private boolean enabled;
  private String endpoint;
  private String uiEndpoint;
  private String title;
  private String description;
  private String version;
  private String contact;
  private String contactUrl;
  private String contactEmail;
  private String license;
  private String licenseUrl;

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

    if (json.getValue("enabled") instanceof Boolean) {
      this.enabled = (Boolean) json.getValue("enabled");
    }
    if (json.getValue("endpoint") instanceof String) {
      this.endpoint = (String) json.getValue("endpoint");
    }
    if (json.getValue("uiEndpoint") instanceof String) {
      this.uiEndpoint = (String) json.getValue("uiEndpoint");
    }
    if (json.getValue("title") instanceof String) {
      this.title = (String) json.getValue("title");
    }
    if (json.getValue("description") instanceof String) {
      this.description = (String) json.getValue("description");
    }
    if (json.getValue("version") instanceof String) {
      this.version = (String) json.getValue("version");
    }
    if (json.getValue("contact") instanceof String) {
      this.contact = (String) json.getValue("contact");
    }
    if (json.getValue("contactUrl") instanceof String) {
      this.contactUrl = (String) json.getValue("contactUrl");
    }
    if (json.getValue("contactEmail") instanceof String) {
      this.contactEmail = (String) json.getValue("contactEmail");
    }
    if (json.getValue("license") instanceof String) {
      this.license = (String) json.getValue("license");
    }
    if (json.getValue("licenseUrl") instanceof String) {
      this.licenseUrl = (String) json.getValue("licenseUrl");
    }
  }
}

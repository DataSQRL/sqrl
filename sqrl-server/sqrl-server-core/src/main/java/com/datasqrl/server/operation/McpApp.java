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
package com.datasqrl.server.operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.NonNull;

/**
 * An MCP app: an interactive HTML UI that MCP hosts with the MCP Apps extension ({@value
 * #EXTENSION_ID}) render for the tools linked to it. It is served as the MCP resource {@code uri}.
 *
 * @param name the name of the app, which is its file name without {@code .html}
 * @param uri the {@code ui://} URI of the resource
 * @param mimeType the MIME type of the resource
 * @param content the self-contained HTML document
 */
public record McpApp(
    @JsonProperty("name") @NonNull String name,
    @JsonProperty("uri") @NonNull String uri,
    @JsonProperty("mimeType") @NonNull String mimeType,
    @JsonProperty("content") @NonNull String content) {

  /** The identifier of the MCP Apps extension. */
  public static final String EXTENSION_ID = "io.modelcontextprotocol/ui";

  /** The MIME type of MCP app resources. */
  public static final String MIME_TYPE = "text/html;profile=mcp-app";

  /** Returns the app with the given name and content, at its {@code ui://apps/<name>} URI. */
  public static McpApp of(String name, String content) {
    return new McpApp(name, "ui://apps/" + name, MIME_TYPE, content);
  }
}

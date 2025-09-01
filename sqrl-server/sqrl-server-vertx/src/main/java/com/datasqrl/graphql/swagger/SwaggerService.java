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
package com.datasqrl.graphql.swagger;

import com.datasqrl.graphql.config.SwaggerConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.servers.Server;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class SwaggerService {

  private static final Pattern QUERY_PARAMS_PATTERN = Pattern.compile("\\{\\?([^}]+)\\}");
  private static final Pattern PATH_PARAMS_PATTERN = Pattern.compile("\\{([^}?]+)\\}");
  private static final ObjectMapper objectMapper = Json.mapper();

  private final SwaggerConfig swaggerConfig;
  private final RootGraphqlModel model;
  private final String modelVersion;
  private final String restEndpoint;

  public String generateSwaggerJson(String requestHost) {
    try {
      var openAPI = createOpenAPI(requestHost);
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(openAPI);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate Swagger JSON", e);
      return "{}";
    }
  }

  private OpenAPI createOpenAPI(String requestHost) {
    var openAPI = new OpenAPI();

    // Set API info
    var info =
        new Info()
            .title(swaggerConfig.getTitle())
            .description(swaggerConfig.getDescription())
            .version(swaggerConfig.getVersion());

    if (swaggerConfig.getContact() != null) {
      var contact =
          new Contact()
              .name(swaggerConfig.getContact())
              .url(swaggerConfig.getContactUrl())
              .email(swaggerConfig.getContactEmail());
      info.contact(contact);
    }

    if (swaggerConfig.getLicense() != null) {
      var license =
          new License().name(swaggerConfig.getLicense()).url(swaggerConfig.getLicenseUrl());
      info.license(license);
    }

    openAPI.info(info);

    // Add server based on request host
    var serverUrl = requestHost != null ? requestHost : "http://localhost:8888";
    var server = new Server().url(serverUrl).description("DataSQRL API Server");
    openAPI.addServersItem(server);

    // Generate paths from REST operations
    var paths = new Paths();
    for (ApiOperation operation : model.getOperations()) {
      if (operation.isRestEndpoint()) {
        addOperationToPath(paths, operation);
      }
    }
    openAPI.paths(paths);

    return openAPI;
  }

  private void addOperationToPath(Paths paths, ApiOperation operation) {
    var uriTemplate = operation.getUriTemplate();
    var httpMethod = operation.getRestMethod();

    if (uriTemplate == null || httpMethod == null) {
      return;
    }

    // Convert URI template to OpenAPI path
    var pathPattern = convertUriTemplateToOpenApiPath(uriTemplate);

    var pathItem = paths.get(pathPattern);
    if (pathItem == null) {
      pathItem = new PathItem();
      paths.put(pathPattern, pathItem);
    }

    var swaggerOperation = createSwaggerOperation(operation, uriTemplate);

    switch (httpMethod) {
      case GET -> pathItem.get(swaggerOperation);
      case POST -> pathItem.post(swaggerOperation);
      case NONE -> throw new UnsupportedOperationException("Should not be called");
    }
  }

  private String convertUriTemplateToOpenApiPath(String uriTemplate) {
    // Remove query parameters pattern {?param1,param2}
    var path = QUERY_PARAMS_PATTERN.matcher(uriTemplate).replaceAll("");

    // Convert path parameters {param} to {param} (same format)
    // No change needed for OpenAPI format

    // Ensure path starts with /
    if (!path.startsWith("/")) {
      path = "/" + path;
    }

    // Add REST endpoint prefix to match actual server routes
    path = restEndpoint + path;

    return path;
  }

  private Operation createSwaggerOperation(ApiOperation operation, String uriTemplate) {
    var swaggerOperation =
        new Operation()
            .operationId(operation.getName())
            .summary(operation.getName())
            .description(operation.getFunction().getDescription());

    // Add parameters
    var parameters = extractParameters(uriTemplate);
    if (!parameters.isEmpty()) {
      swaggerOperation.parameters(parameters);
    }

    // Add responses
    var responses = new ApiResponses();

    // Success response
    var successResponse =
        new ApiResponse()
            .description("Successful operation")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                new Schema<>()
                                    .type("object")
                                    .addProperty(
                                        "data",
                                        new Schema<>()
                                            .type("object")
                                            .description("Response data")))));
    responses.addApiResponse("200", successResponse);

    // Error response
    var errorResponse =
        new ApiResponse()
            .description("Error response")
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType()
                            .schema(
                                new Schema<>()
                                    .type("object")
                                    .addProperty(
                                        "errors",
                                        new Schema<>()
                                            .type("array")
                                            .items(new Schema<>().type("object"))))));
    responses.addApiResponse("400", errorResponse);

    swaggerOperation.responses(responses);

    return swaggerOperation;
  }

  private List<Parameter> extractParameters(String uriTemplate) {
    List<Parameter> parameters = new ArrayList<>();

    // Extract path parameters
    var pathMatcher = PATH_PARAMS_PATTERN.matcher(uriTemplate);
    while (pathMatcher.find()) {
      var paramName = pathMatcher.group(1);
      if (!paramName.contains("?")) { // Skip query parameter syntax
        var parameter =
            new Parameter()
                .name(paramName)
                .in("path")
                .required(true)
                .schema(new Schema<>().type("string"));
        parameters.add(parameter);
      }
    }

    // Extract query parameters
    var queryMatcher = QUERY_PARAMS_PATTERN.matcher(uriTemplate);
    while (queryMatcher.find()) {
      var queryParams = queryMatcher.group(1);
      var paramNames = queryParams.split(",");
      for (String paramName : paramNames) {
        var parameter =
            new Parameter()
                .name(paramName.trim())
                .in("query")
                .required(false)
                .schema(new Schema<>().type("string"));
        parameters.add(parameter);
      }
    }

    return parameters;
  }

  public String generateSwaggerUI() {
    var swaggerUIHtml =
        """
        <!DOCTYPE html>
        <html>
        <head>
            <title>%s</title>
            <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@latest/swagger-ui.css" />
            <style>
                html {
                    box-sizing: border-box;
                    overflow: -moz-scrollbars-vertical;
                    overflow-y: scroll;
                }
                *, *:before, *:after {
                    box-sizing: inherit;
                }
                body {
                    margin:0;
                    background: #fafafa;
                }
            </style>
        </head>
        <body>
            <div id="swagger-ui"></div>
            <script src="https://unpkg.com/swagger-ui-dist@latest/swagger-ui-bundle.js"></script>
            <script src="https://unpkg.com/swagger-ui-dist@latest/swagger-ui-standalone-preset.js"></script>
            <script>
                window.onload = function() {
                    SwaggerUIBundle({
                        url: '%s',
                        dom_id: '#swagger-ui',
                        deepLinking: true,
                        presets: [
                            SwaggerUIBundle.presets.apis,
                            SwaggerUIStandalonePreset
                        ],
                        plugins: [
                            SwaggerUIBundle.plugins.DownloadUrl
                        ],
                        layout: "StandaloneLayout"
                    });
                }
            </script>
        </body>
        </html>
        """;

    return String.format(
        swaggerUIHtml, swaggerConfig.getTitle(), swaggerConfig.getEndpoint(modelVersion));
  }
}

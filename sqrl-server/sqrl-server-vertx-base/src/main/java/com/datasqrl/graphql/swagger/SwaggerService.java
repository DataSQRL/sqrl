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
import com.datasqrl.graphql.server.operation.RestMethodType;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SwaggerService {

  private static final Pattern QUERY_PARAMS_PATTERN = Pattern.compile("\\{\\?([^}]+)\\}");
  private static final Pattern PATH_PARAMS_PATTERN = Pattern.compile("\\{([^}?]+)\\}");
  private static final ObjectMapper objectMapper = Json.mapper();

  private final SwaggerConfig swaggerConfig;
  private final RootGraphqlModel model;
  private final String serverBaseUrl;

  public SwaggerService(SwaggerConfig swaggerConfig, RootGraphqlModel model, String serverBaseUrl) {
    this.swaggerConfig = swaggerConfig;
    this.model = model;
    this.serverBaseUrl = serverBaseUrl;
  }

  public String generateSwaggerJson() {
    try {
      OpenAPI openAPI = createOpenAPI();
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(openAPI);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate Swagger JSON", e);
      return "{}";
    }
  }

  private OpenAPI createOpenAPI() {
    OpenAPI openAPI = new OpenAPI();

    // Set API info
    Info info =
        new Info()
            .title(swaggerConfig.getTitle())
            .description(swaggerConfig.getDescription())
            .version(swaggerConfig.getVersion());

    if (swaggerConfig.getContact() != null) {
      Contact contact =
          new Contact()
              .name(swaggerConfig.getContact())
              .url(swaggerConfig.getContactUrl())
              .email(swaggerConfig.getContactEmail());
      info.contact(contact);
    }

    if (swaggerConfig.getLicense() != null) {
      License license =
          new License().name(swaggerConfig.getLicense()).url(swaggerConfig.getLicenseUrl());
      info.license(license);
    }

    openAPI.info(info);

    // Add server
    Server server = new Server().url(serverBaseUrl).description("DataSQRL API Server");
    openAPI.addServersItem(server);

    // Generate paths from REST operations
    Paths paths = new Paths();
    for (ApiOperation operation : model.getOperations()) {
      if (operation.isRestEndpoint()) {
        addOperationToPath(paths, operation);
      }
    }
    openAPI.paths(paths);

    return openAPI;
  }

  private void addOperationToPath(Paths paths, ApiOperation operation) {
    String uriTemplate = operation.getUriTemplate();
    RestMethodType httpMethod = operation.getRestMethod();

    if (uriTemplate == null || httpMethod == null) {
      return;
    }

    // Convert URI template to OpenAPI path
    String pathPattern = convertUriTemplateToOpenApiPath(uriTemplate);

    PathItem pathItem = paths.get(pathPattern);
    if (pathItem == null) {
      pathItem = new PathItem();
      paths.put(pathPattern, pathItem);
    }

    Operation swaggerOperation = createSwaggerOperation(operation, uriTemplate);

    switch (httpMethod) {
      case GET -> pathItem.get(swaggerOperation);
      case POST -> pathItem.post(swaggerOperation);
      case NONE -> throw new UnsupportedOperationException("Should not be called");
    }
  }

  private String convertUriTemplateToOpenApiPath(String uriTemplate) {
    // Remove query parameters pattern {?param1,param2}
    String path = QUERY_PARAMS_PATTERN.matcher(uriTemplate).replaceAll("");

    // Convert path parameters {param} to {param} (same format)
    // No change needed for OpenAPI format

    // Ensure path starts with /
    if (!path.startsWith("/")) {
      path = "/" + path;
    }

    return path;
  }

  private Operation createSwaggerOperation(ApiOperation operation, String uriTemplate) {
    Operation swaggerOperation =
        new Operation()
            .operationId(operation.getName())
            .summary(operation.getName())
            .description("Auto-generated REST endpoint for " + operation.getName());

    // Add parameters
    List<Parameter> parameters = extractParameters(uriTemplate);
    if (!parameters.isEmpty()) {
      swaggerOperation.parameters(parameters);
    }

    // Add responses
    ApiResponses responses = new ApiResponses();

    // Success response
    ApiResponse successResponse =
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
    ApiResponse errorResponse =
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
    Matcher pathMatcher = PATH_PARAMS_PATTERN.matcher(uriTemplate);
    while (pathMatcher.find()) {
      String paramName = pathMatcher.group(1);
      if (!paramName.contains("?")) { // Skip query parameter syntax
        Parameter parameter =
            new Parameter()
                .name(paramName)
                .in("path")
                .required(true)
                .schema(new Schema<>().type("string"));
        parameters.add(parameter);
      }
    }

    // Extract query parameters
    Matcher queryMatcher = QUERY_PARAMS_PATTERN.matcher(uriTemplate);
    while (queryMatcher.find()) {
      String queryParams = queryMatcher.group(1);
      String[] paramNames = queryParams.split(",");
      for (String paramName : paramNames) {
        Parameter parameter =
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
    String swaggerUIHtml =
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

    return String.format(swaggerUIHtml, swaggerConfig.getTitle(), swaggerConfig.getEndpoint());
  }
}

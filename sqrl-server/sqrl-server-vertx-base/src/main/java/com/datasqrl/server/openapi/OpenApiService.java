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
package com.datasqrl.server.openapi;

import com.datasqrl.server.config.OpenApiConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.server.operation.FunctionDefinition;
import com.datasqrl.server.operation.RestMethodType;
import com.datasqrl.server.operation.ResultDefinition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.servers.Server;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

@RequiredArgsConstructor
@Slf4j
public class OpenApiService {

  public static final String OPENAPI_JSON_ARTIFACT_NAME_SUFFIX_TEMPLATE = "-%s-openapi.json";

  private static final Pattern QUERY_PARAMS_PATTERN = Pattern.compile("\\{\\?([^}]+)\\}");
  private static final Pattern PATH_PARAMS_PATTERN = Pattern.compile("\\{([^}?]+)\\}");
  private static final ObjectMapper objectMapper = Json.mapper();

  private final OpenApiConfig openApiConfig;
  private final RootGraphQLModel model;
  private final String modelVersion;
  private final String restEndpoint;

  public String generateOpenApiJson() {
    return generateOpenApiJson(null);
  }

  public String generateOpenApiJson(String requestHost) {
    try {
      var openAPI = createOpenAPI(requestHost);
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(openAPI);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate OpenAPI JSON", e);
      return "{}";
    }
  }

  public void validateOpenApiJson(String openApiJson) {
    try {
      var openApi = objectMapper.readTree(openApiJson);
      if (!(openApi instanceof ObjectNode openApiObject)
          || !openApiObject.path("openapi").isTextual()
          || !openApiObject.path("info").isObject()
          || !openApiObject.path("paths").isObject()) {
        throw new IllegalArgumentException("OpenAPI artifact is not a valid OpenAPI document");
      }
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("OpenAPI artifact is not valid JSON", e);
    }
  }

  /**
   * Replaces the deployment-specific server URL without regenerating the compiled specification.
   */
  public String withRequestHost(String openApiJson, String requestHost) {
    if (StringUtils.isBlank(requestHost)) {
      return openApiJson;
    }
    try {
      var openApi = objectMapper.readTree(openApiJson);
      if (!(openApi instanceof ObjectNode openApiObject)) {
        throw new IllegalArgumentException("OpenAPI artifact must contain a JSON object");
      }
      ArrayNode servers = openApiObject.withArray("servers");
      if (servers.isEmpty()) {
        servers.addObject().put("url", requestHost).put("description", "DataSQRL API Server");
      } else if (servers.get(0) instanceof ObjectNode server) {
        server.put("url", requestHost);
      } else {
        throw new IllegalArgumentException("OpenAPI artifact server must be a JSON object");
      }
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(openApi);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to update OpenAPI artifact server URL", e);
    }
  }

  private OpenAPI createOpenAPI(String requestHost) {
    var openAPI = new OpenAPI();

    // Set API info
    var info =
        new Info()
            .title(openApiConfig.getTitle())
            .description(openApiConfig.getDescription())
            .version(openApiConfig.getVersion());

    if (openApiConfig.getContact() != null) {
      var contact =
          new Contact()
              .name(openApiConfig.getContact())
              .url(openApiConfig.getContactUrl())
              .email(openApiConfig.getContactEmail());
      info.contact(contact);
    }

    if (openApiConfig.getLicense() != null) {
      var license =
          new License().name(openApiConfig.getLicense()).url(openApiConfig.getLicenseUrl());
      info.license(license);
    }

    openAPI.info(info);

    var serverUrl = StringUtils.defaultIfBlank(requestHost, "http://localhost:8888");
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
    var pathItem = paths.computeIfAbsent(pathPattern, k -> new PathItem());

    var openApiOperation = createOpenApiOperation(operation, uriTemplate);

    switch (httpMethod) {
      case GET -> pathItem.get(openApiOperation);
      case POST -> pathItem.post(openApiOperation);
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

  private Operation createOpenApiOperation(ApiOperation operation, String uriTemplate) {
    var description = operation.getFunction().getDescription();

    var openApiOperation =
        new Operation()
            .operationId(operation.getName())
            .summary(operation.getName())
            .description(description);

    if (StringUtils.isNotBlank(description)) {
      openApiOperation.summary(getSummary(description));
    }

    // Add parameters
    var parameters = extractParameters(uriTemplate);
    if (!parameters.isEmpty()) {
      openApiOperation.parameters(parameters);
    }

    // Add request body
    if (operation.getRestMethod() == RestMethodType.POST) {
      var requestBody = buildRequestBody(operation);
      requestBody.ifPresent(openApiOperation::setRequestBody);
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
                                        "data", resultToSchema(operation.getResultDefinition())))));
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

    openApiOperation.responses(responses);

    return openApiOperation;
  }

  private Schema<?> resultToSchema(ResultDefinition result) {
    if (result == null) {
      return new Schema<>().type("object").description("Response data");
    }
    var schema = new Schema<>();
    schema.type(result.getType());
    schema.format(result.getFormat());
    schema.description(result.getDescription());
    if (result.getEnumValues() != null) {
      schema._enum(new ArrayList<>(result.getEnumValues()));
    }
    if (result.getItems() != null) {
      schema.items(resultToSchema(result.getItems()));
    }
    if (ObjectUtils.isNotEmpty(result.getProperties())) {
      for (var entry : result.getProperties().entrySet()) {
        schema.addProperty(entry.getKey(), resultToSchema(entry.getValue()));
      }
    }
    return schema;
  }

  private String getSummary(String description) {
    var firstLine = description.lines().findFirst().orElse("").trim();

    return firstLine.substring(0, Math.min(firstLine.length(), 120));
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

  private Optional<RequestBody> buildRequestBody(ApiOperation operation) {
    var fn = operation.getFunction();
    var params = fn.getParameters();
    var props = params.getProperties();

    if (props.isEmpty()) {
      return Optional.empty();
    }

    var requestBody = new RequestBody();
    requestBody.description(fn.getDescription());
    requestBody.content(
        new Content()
            .addMediaType("application/json", new MediaType().schema(paramsToSchema(params))));

    return Optional.of(requestBody);
  }

  private Schema<?> paramsToSchema(FunctionDefinition.Parameters params) {
    var schema = new Schema<>();
    schema.type(params.getType());

    addPropsToSchema(schema, params.getProperties());

    if (ObjectUtils.isNotEmpty(params.getRequired())) {
      schema.required(params.getRequired());
    }

    return schema;
  }

  private Schema<?> argToSchema(FunctionDefinition.Argument arg) {
    var schema = new Schema<>();
    schema.type(arg.getType());
    schema.description(arg.getDescription());

    if (ObjectUtils.isNotEmpty(arg.getEnumValues())) {
      schema._enum(new ArrayList<>(arg.getEnumValues()));
    }

    if (arg.getItems() != null) {
      schema.items(argToSchema(arg.getItems()));
    }

    addPropsToSchema(schema, arg.getProperties());

    if (ObjectUtils.isNotEmpty(arg.getRequired())) {
      schema.required(arg.getRequired());
    }

    return schema;
  }

  private void addPropsToSchema(Schema<?> schema, Map<String, FunctionDefinition.Argument> props) {
    if (ObjectUtils.isNotEmpty(props)) {
      for (var entry : props.entrySet()) {
        schema.addProperty(entry.getKey(), argToSchema(entry.getValue()));
      }
    }
  }

  public String generateSwaggerUi() {
    var openApiUiHtml =
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
        openApiUiHtml, openApiConfig.getTitle(), openApiConfig.getEndpoint(modelVersion));
  }
}

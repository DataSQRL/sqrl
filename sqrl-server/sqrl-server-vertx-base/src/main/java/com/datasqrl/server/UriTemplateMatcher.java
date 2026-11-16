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
package com.datasqrl.server;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Matches URIs against an RFC 6570 template with path expressions ({@code {id}}) and a query
 * expression ({@code {?a,b}}).
 */
public class UriTemplateMatcher {

  private static final Pattern QUERY_PARAMS_PATTERN = Pattern.compile("\\{\\?([^}]+)\\}");
  private static final Pattern PATH_PARAMS_PATTERN = Pattern.compile("\\{([^}?]+)\\}");

  private final Pattern uriPattern;
  private final List<String> pathParameterNames = new ArrayList<>();

  public UriTemplateMatcher(String uriTemplate) {
    var pathTemplate = removeQueryExpressions(uriTemplate);
    var regex = new StringBuilder("^");
    var matcher = PATH_PARAMS_PATTERN.matcher(pathTemplate);
    var literalStart = 0;
    while (matcher.find()) {
      regex.append(Pattern.quote(pathTemplate.substring(literalStart, matcher.start())));
      regex.append("([^/?#]+)");
      pathParameterNames.add(matcher.group(1));
      literalStart = matcher.end();
    }
    regex.append(Pattern.quote(pathTemplate.substring(literalStart)));
    // Like the REST bridge, any query parameter is accepted, not only those in the template.
    regex.append("(?:\\?([^#]*))?$");
    uriPattern = Pattern.compile(regex.toString());
  }

  /**
   * Converts the template to a Vert.x route path, e.g. {@code users/{id}{?limit}} to /users/:id.
   */
  public static String toRoutePath(String uriTemplate) {
    var route = PATH_PARAMS_PATTERN.matcher(removeQueryExpressions(uriTemplate)).replaceAll(":$1");
    return route.startsWith("/") ? route : "/" + route;
  }

  /**
   * Returns the decoded parameters when the URI matches the template.
   *
   * @throws IllegalArgumentException when a parameter has an invalid percent-encoding
   */
  public Optional<Match> match(String uri) {
    var matcher = uriPattern.matcher(uri);
    if (!matcher.matches()) {
      return Optional.empty();
    }
    var pathParameters = new LinkedHashMap<String, String>();
    for (var i = 0; i < pathParameterNames.size(); i++) {
      // '+' is a literal in a path segment, unlike in a query string
      pathParameters.put(
          pathParameterNames.get(i), decode(matcher.group(i + 1).replace("+", "%2B")));
    }
    var query = matcher.group(pathParameterNames.size() + 1);
    return Optional.of(new Match(pathParameters, parseQuery(query)));
  }

  private static String removeQueryExpressions(String uriTemplate) {
    return QUERY_PARAMS_PATTERN.matcher(uriTemplate).replaceAll("");
  }

  private static Map<String, String> parseQuery(String query) {
    var parameters = new LinkedHashMap<String, String>();
    if (query == null || query.isEmpty()) {
      return parameters;
    }
    for (var pair : query.split("&")) {
      if (pair.isEmpty()) {
        continue;
      }
      var separator = pair.indexOf('=');
      var name = decode(separator < 0 ? pair : pair.substring(0, separator));
      var value = separator < 0 ? "" : decode(pair.substring(separator + 1));
      parameters.putIfAbsent(name, value);
    }
    return parameters;
  }

  private static String decode(String value) {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }

  public record Match(Map<String, String> pathParameters, Map<String, String> queryParameters) {}
}

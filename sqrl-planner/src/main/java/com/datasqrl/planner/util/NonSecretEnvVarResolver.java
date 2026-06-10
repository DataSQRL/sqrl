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
package com.datasqrl.planner.util;

import com.datasqrl.flinkrunner.utils.EnvVarResolver;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.SuperBuilder;

/** Resolves regular environment variables and converts secret templates to standard templates. */
@SuperBuilder
public class NonSecretEnvVarResolver extends EnvVarResolver {

  private static final Pattern SECRET_ENVIRONMENT_VARIABLE_PATTERN =
      Pattern.compile("\\$\\{\\{(.*?)\\}\\}");

  private static final Pattern SECRET_ENVIRONMENT_VARIABLE_NAME_PATTERN =
      Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  @Override
  public String resolve(String src) {
    if (src == null || src.isBlank()) {
      return src;
    }

    var resolvedRegularEnvVars = super.resolve(src);

    return resolveSecretEnvVars(resolvedRegularEnvVars);
  }

  private String resolveSecretEnvVars(String src) {
    var res = new StringBuilder();
    var matcher = SECRET_ENVIRONMENT_VARIABLE_PATTERN.matcher(src);
    while (matcher.find()) {
      var key = matcher.group(1);
      if (!SECRET_ENVIRONMENT_VARIABLE_NAME_PATTERN.matcher(key).matches()) {
        throw new IllegalArgumentException(
            "Secret environment variable templates only support variable names, but found: %s"
                .formatted(matcher.group()));
      }

      matcher.appendReplacement(res, Matcher.quoteReplacement(String.format("${%s}", key)));
    }
    matcher.appendTail(res);

    return res.toString();
  }
}

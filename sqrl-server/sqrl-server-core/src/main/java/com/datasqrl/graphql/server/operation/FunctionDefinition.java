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
package com.datasqrl.graphql.server.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Definition of a chat function that can be invoked by the language model.
 *
 * <p>This is essentially a java definition of the json structure OpenAI and most LLMs use to
 * represent functions/tools.
 *
 * <p>This should be updated whenever the model representation of a function changes. It should not
 * contain any extra functionality - those should be added to the respective wrapper classes.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FunctionDefinition {

  private String name;
  private String description;
  private Parameters parameters;

  @Data
  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Parameters {

    private String type;
    private Map<String, Argument> properties = Map.of();
    private List<String> required = List.of();

    @JsonIgnore
    public boolean isNested() {
      return properties.values().stream()
          .anyMatch(
              arg ->
                  arg.getProperties() != null
                      || (arg.getItems() != null && arg.getItems().getProperties() != null));
    }
  }

  @Data
  public static class Argument {

    private String type;
    private String description;
    private Argument items;
    private Map<String, Argument> properties;
    private List<String> required;

    @JsonProperty("enum")
    private Set<?> enumValues;
  }
}

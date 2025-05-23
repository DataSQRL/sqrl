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
package com.datasqrl.ai.tool;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The {@link ContextImpl} class captures the context of an agent interaction. It has a request id
 * that is unique for each interaction and an invocation counter for the number of times the LLM is
 * invoked in the course of producing a request response.
 *
 * <p>Additional key-value pairs can be provided to securely pass information to the function calls
 * outside the LLM call stack.
 *
 * <p>The request id and secure information are static for the duration of an interaction. The
 * counter is incremented for each time the LLM is invoked.
 */
@AllArgsConstructor
@Getter
public class ContextImpl implements Context {

  private final Map<String, Object> secure;

  public Object get(String key) {
    return secure.get(key);
  }

  public void forEach(BiConsumer<String, Object> action) {
    Objects.requireNonNull(action);
    secure.forEach(action);
  }

  public Map<String, Object> asMap() {
    Map<String, Object> result = new HashMap<>(secure.size() + 2);
    forEach(result::put);
    return result;
  }
}

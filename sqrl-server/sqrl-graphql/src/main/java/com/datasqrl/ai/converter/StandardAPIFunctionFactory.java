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
package com.datasqrl.ai.converter;

import com.datasqrl.ai.api.APIQuery;
import com.datasqrl.ai.api.APIQueryExecutor;
import com.datasqrl.ai.tool.APIFunction;
import com.datasqrl.ai.tool.FunctionDefinition;
import java.util.Set;

/**
 * This is the factory class for creating APIFunction instances. It implements the
 * APIFunctionFactory interface. The class is implemented using Java Records feature, which is a
 * final immutable class by default. Being a factory class, its main responsibility is to generate
 * and return instances of APIFunction class.
 *
 * @param apiExecutor is a APIQueryExecutor type object which executes the APIQuery.
 * @param contextKeys is a set of Strings which are considered as keys in the context of this
 *     APIFunctionFactory.
 *     <p>Note: Any change in the fields of this class or method definitions will affect the objects
 *     created by this factory.
 */
public record StandardAPIFunctionFactory(APIQueryExecutor apiExecutor, Set<String> contextKeys)
    implements APIFunctionFactory {

  @Override
  public APIFunction create(FunctionDefinition function, APIQuery query) {
    return new APIFunction(function, contextKeys, query, apiExecutor);
  }
}

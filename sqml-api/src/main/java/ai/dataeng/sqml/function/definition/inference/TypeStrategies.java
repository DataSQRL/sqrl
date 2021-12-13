/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.dataeng.sqml.function.definition.inference;


import ai.dataeng.sqml.function.definition.inference.strategies.ExplicitTypeStrategy;
import ai.dataeng.sqml.schema2.Type;


/**
 * Strategies for inferring an output or accumulator data type of a function call.
 *
 * @see TypeStrategy
 */
public final class TypeStrategies {

    /** Type strategy that returns a fixed {@link Type}. */
    public static TypeStrategy explicit(Type dataType) {
        return new ExplicitTypeStrategy(dataType);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private TypeStrategies() {
        // no instantiation
    }
}

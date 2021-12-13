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

import ai.dataeng.sqml.function.definition.inference.strategies.AnyArgumentTypeStrategy;
import ai.dataeng.sqml.function.definition.inference.strategies.SequenceInputTypeStrategy;
import ai.dataeng.sqml.function.definition.inference.strategies.WildcardInputTypeStrategy;
import java.util.Arrays;

/**
 * Strategies for inferring and validating input arguments in a function call.
 *
 * @see InputTypeStrategy
 * @see ArgumentTypeStrategy
 */
public final class InputTypeStrategies {
    /** Strategy that does not perform any modification or validation of the input. */
    public static final WildcardInputTypeStrategy WILDCARD = new WildcardInputTypeStrategy();

    /** Strategy that does not expect any arguments. */
    public static final InputTypeStrategy NO_ARGS = sequence();

    /**
     * Strategy for a function signature like {@code f(STRING, NUMERIC)} using a sequence of {@link
     * ArgumentTypeStrategy}s.
     */
    public static InputTypeStrategy sequence(ArgumentTypeStrategy... strategies) {
        return new SequenceInputTypeStrategy(Arrays.asList(strategies), null);
    }

    /** Strategy for an argument that can be of any type. */
    public static final AnyArgumentTypeStrategy ANY = new AnyArgumentTypeStrategy();

    // --------------------------------------------------------------------------------------------

    private InputTypeStrategies() {
        // no instantiation
    }
}

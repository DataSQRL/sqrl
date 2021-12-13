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

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;


/**
 * Provides logic for the type inference of function calls. It includes:
 *
 * <ul>
 *   <li>explicit input specification for (possibly named and/or typed) arguments
 *   <li>inference of missing or incomplete input types
 *   <li>validation of input types
 *   <li>inference of an intermediate accumulation type
 *   <li>inference of the final output type
 * </ul>
 *
 * <p>See {@link TypeInferenceUtil} for more information about the type inference process.
 */
public final class TypeInference {
    private final InputTypeStrategy inputTypeStrategy;
    private final TypeStrategy outputTypeStrategy;

    private TypeInference(
            InputTypeStrategy inputTypeStrategy,
            TypeStrategy outputTypeStrategy) {
        this.inputTypeStrategy = inputTypeStrategy;
        this.outputTypeStrategy = outputTypeStrategy;
    }

    /** Builder for configuring and creating instances of {@link TypeInference}. */
    public static TypeInference.Builder newBuilder() {
        return new TypeInference.Builder();
    }

    public InputTypeStrategy getInputTypeStrategy() {
        return inputTypeStrategy;
    }

    public TypeStrategy getOutputTypeStrategy() {
        return outputTypeStrategy;
    }

    /** Builder for configuring and creating instances of {@link TypeInference}. */
    public static class Builder {
        private InputTypeStrategy inputTypeStrategy = InputTypeStrategies.WILDCARD;

        private @Nullable TypeStrategy outputTypeStrategy;

        public Builder() {
            // default constructor to allow a fluent definition
        }

        /**
         * Sets the strategy for inferring and validating input arguments in a function call.
         *
         * <p>A {@link InputTypeStrategies#WILDCARD} strategy function is assumed by default.
         */
        public Builder inputTypeStrategy(InputTypeStrategy inputTypeStrategy) {
            this.inputTypeStrategy =
                    Preconditions.checkNotNull(
                            inputTypeStrategy, "Input type strategy must not be null.");
            return this;
        }

        /**
         * Sets the strategy for inferring the final output data type of a function call.
         *
         * <p>Required.
         */
        public Builder outputTypeStrategy(TypeStrategy outputTypeStrategy) {
            this.outputTypeStrategy =
                    Preconditions.checkNotNull(
                            outputTypeStrategy, "Output type strategy must not be null.");
            return this;
        }

        public TypeInference build() {
            return new TypeInference(
                    inputTypeStrategy,
                    Preconditions.checkNotNull(
                            outputTypeStrategy, "Output type strategy must not be null."));
        }
    }
}

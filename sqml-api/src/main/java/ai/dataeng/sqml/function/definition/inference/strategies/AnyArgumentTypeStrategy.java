/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.dataeng.sqml.function.definition.inference.strategies;

import ai.dataeng.sqml.function.definition.FunctionDefinition;
import ai.dataeng.sqml.function.definition.inference.ArgumentTypeStrategy;
import ai.dataeng.sqml.function.definition.inference.CallContext;
import ai.dataeng.sqml.function.definition.inference.Signature;
import ai.dataeng.sqml.schema2.Type;
import java.util.Optional;


/** Strategy for an argument that can be of any type. */
public final class AnyArgumentTypeStrategy implements ArgumentTypeStrategy {

    @Override
    public Optional<Type> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        return Optional.of(callContext.getArgumentDataTypes().get(argumentPos));
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        return Signature.Argument.of("<ANY>");
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof org.apache.flink.table.types.inference.strategies.AnyArgumentTypeStrategy;
    }

    @Override
    public int hashCode() {
        return org.apache.flink.table.types.inference.strategies.AnyArgumentTypeStrategy.class.hashCode();
    }
}

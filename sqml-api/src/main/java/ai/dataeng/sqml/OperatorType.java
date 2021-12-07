/*
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
package ai.dataeng.sqml;


import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toMap;

public enum OperatorType {
    ADD("+", false),
    SUBTRACT("-", false),
    MULTIPLY("*", false),
    DIVIDE("/", false),
    MODULUS("%", false),
    NEGATION("-", false),
    EQUAL("=", false),
    NOT_EQUAL("<>", false),
    LESS_THAN("<", false),
    LESS_THAN_OR_EQUAL("<=", false),
    GREATER_THAN(">", false),
    GREATER_THAN_OR_EQUAL(">=", false),
    BETWEEN("BETWEEN", false);

    private static final Map<QualifiedObjectName, OperatorType> OPERATOR_TYPES = Arrays.stream(OperatorType.values()).collect(toMap(OperatorType::getFunctionName, Function.identity()));

    private final String operator;
    private final QualifiedObjectName functionName;
    private final boolean calledOnNullInput;

    OperatorType(String operator, boolean calledOnNullInput)
    {
        this.operator = operator;
        this.functionName = QualifiedObjectName.valueOf("presto", "default", "$operator$" + name());
        this.calledOnNullInput = calledOnNullInput;
    }

    public String getOperator()
    {
        return operator;
    }

    public QualifiedObjectName getFunctionName()
    {
        return functionName;
    }

    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }

    public static Optional<OperatorType> tryGetOperatorType(QualifiedObjectName operatorName)
    {
        return Optional.ofNullable(OPERATOR_TYPES.get(operatorName));
    }

    public boolean isComparisonOperator()
    {
        return this.equals(EQUAL) ||
                this.equals(NOT_EQUAL) ||
                this.equals(LESS_THAN) ||
                this.equals(LESS_THAN_OR_EQUAL) ||
                this.equals(GREATER_THAN) ||
                this.equals(GREATER_THAN_OR_EQUAL);
    }

    public boolean isArithmeticOperator()
    {
        return this.equals(ADD) || this.equals(SUBTRACT) || this.equals(MULTIPLY) || this.equals(DIVIDE) || this.equals(MODULUS);
    }
    public static class QualifiedObjectName {
        private String catalogName;
        private String schemaName;
        private String objectName;

        public QualifiedObjectName(String catalogName, String schemaName, String objectName) {

            this.catalogName = catalogName;
            this.schemaName = schemaName;
            this.objectName = objectName;
        }

        public static QualifiedObjectName valueOf(String catalogName, String schemaName, String objectName)
        {
            return new QualifiedObjectName(catalogName, schemaName, objectName.toLowerCase(ENGLISH));
        }

    }
}

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
package ai.dataeng.sqml.relation;

import static ai.dataeng.sqml.relation.Expressions.constant;
import static ai.dataeng.sqml.relation.Expressions.constantNull;
import static ai.dataeng.sqml.relation.Expressions.specialForm;
import static ai.dataeng.sqml.relation.SpecialFormExpression.Form.AND;
import static ai.dataeng.sqml.relation.SpecialFormExpression.Form.OR;

import ai.dataeng.sqml.relation.SpecialFormExpression.Form;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BigIntegerType;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.schema2.basic.FloatType;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.schema2.basic.NullType;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.BooleanLiteral;
import ai.dataeng.sqml.tree.DecimalLiteral;
import ai.dataeng.sqml.tree.DoubleLiteral;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FieldReference;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.NullLiteral;
import java.util.List;
import java.util.Map;

public final class SqlToRowExpressionTranslator
{
    private SqlToRowExpressionTranslator() {}

    public static RowExpression translate(
            Expression expression,
            Map<NodeRef<Expression>, Type> types
    )
    {
        Visitor visitor = new Visitor(
                types
        );
        RowExpression result = visitor.process(expression, null);
//        requireNonNull(result, "translated expression is null");
        return result;
    }

    private static class Visitor
            extends AstVisitor<RowExpression, Void>
    {
        private final Map<NodeRef<Expression>, Type> types;

        private Visitor(
                Map<NodeRef<Expression>, Type> types)
        {
            this.types = types;
//            this.functionResolution = new FunctionResolution(functionAndTypeManager);
        }

        private Type getType(Expression node)
        {
            return types.get(NodeRef.of(node));
        }

        @Override
        public RowExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        public RowExpression visitIdentifier(Identifier node, Void context)
        {
            // identifier should never be reachable with the exception of lambda within VALUES (#9711)
            return new VariableReferenceExpression(node.getValue(), getType(node));
        }

        //todo is this used for groups or can we remove this entirely
        @Override
        public RowExpression visitFieldReference(FieldReference node, Void context)
        {
            return null;
//            return field(node.getFieldIndex(), getType(node));
        }

        @Override
        public RowExpression visitNullLiteral(NullLiteral node, Void context)
        {
            return constantNull(NullType.INSTANCE);
        }

        @Override
        public RowExpression visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return constant(node.getValue(), BooleanType.INSTANCE);
        }

        @Override
        public RowExpression visitLongLiteral(LongLiteral node, Void context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return constant(node.getValue(), IntegerType.INSTANCE);
            }
            return constant(node.getValue(), BigIntegerType.INSTANCE);
        }

        @Override
        public RowExpression visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return constant(node.getValue(), FloatType.INSTANCE);
        }

        @Override
        public RowExpression visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            //TODO: We could parse to something other than a double
            Double d = Double.parseDouble(node.getValue());
            return constant(d, FloatType.INSTANCE);
        }
//
//        @Override
//        public RowExpression visitStringLiteral(StringLiteral node, Void context)
//        {
//            return constant(node.getSlice(), createVarcharType(countCodePoints(node.getSlice())));
//        }
//
//        @Override
//        public RowExpression visitEnumLiteral(EnumLiteral node, Void context)
//        {
//            Type type;
//            try {
//                type = functionAndTypeManager.getType(parseTypeSignature(node.getType()));
//            }
//            catch (IllegalArgumentException e) {
//                throw new IllegalArgumentException("Unsupported type: " + node.getType());
//            }
//
//            return constant(node.getValue(), type);
//        }

//        @Override
//        public RowExpression visitGenericLiteral(GenericLiteral node, Void context)
//        {
//            Type type;
//            try {
//                type = functionAndTypeManager.getType(parseTypeSignature(node.getType()));
//            }
//            catch (IllegalArgumentException e) {
//                throw new IllegalArgumentException("Unsupported type: " + node.getType());
//            }
//
//            try {
//                if (TINYINT.equals(type)) {
//                    return constant((long) Byte.parseByte(node.getValue()), TINYINT);
//                }
//                else if (SMALLINT.equals(type)) {
//                    return constant((long) Short.parseShort(node.getValue()), SMALLINT);
//                }
//                else if (BIGINT.equals(type)) {
//                    return constant(Long.parseLong(node.getValue()), BIGINT);
//                }
//            }
//            catch (NumberFormatException e) {
//                throw new SemanticException(SemanticErrorCode.INVALID_LITERAL, node, format("Invalid formatted generic %s literal: %s", type, node));
//            }
//
//            if (JSON.equals(type)) {
//                return call(
//                        "json_parse",
//                        functionAndTypeManager.lookupFunction("json_parse", fromTypes(VARCHAR)),
//                        getType(node),
//                        constant(utf8Slice(node.getValue()), VARCHAR));
//            }
//
//            return call(
//                    CAST.name(),
//                    functionAndTypeManager.lookupCast(CAST, VARCHAR.getTypeSignature(), getType(node).getTypeSignature()),
//                    getType(node),
//                    constant(utf8Slice(node.getValue()), VARCHAR));
//        }
//
//        @Override
//        public RowExpression visitTimeLiteral(TimeLiteral node, Void context)
//        {
//            long value;
//            if (getType(node).equals(TIME_WITH_TIME_ZONE)) {
//                value = parseTimeWithTimeZone(node.getValue());
//            }
//            else {
//                if (sqlFunctionProperties.isLegacyTimestamp()) {
//                    // parse in time zone of client
//                    value = parseTimeWithoutTimeZone(sqlFunctionProperties.getTimeZoneKey(), node.getValue());
//                }
//                else {
//                    value = parseTimeWithoutTimeZone(node.getValue());
//                }
//            }
//            return constant(value, getType(node));
//        }
//
//        @Override
//        public RowExpression visitTimestampLiteral(TimestampLiteral node, Void context)
//        {
//            long value;
//            if (sqlFunctionProperties.isLegacyTimestamp()) {
//                value = parseTimestampLiteral(sqlFunctionProperties.getTimeZoneKey(), node.getValue());
//            }
//            else {
//                value = parseTimestampLiteral(node.getValue());
//            }
//            return constant(value, getType(node));
//        }
//
//        @Override
//        public RowExpression visitIntervalLiteral(IntervalLiteral node, Void context)
//        {
//            long value;
//            if (node.isYearToMonth()) {
//                value = node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
//            }
//            else {
//                value = node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
//            }
//            return constant(value, getType(node));
//        }
//
//        @Override
//        public RowExpression visitComparisonExpression(ComparisonExpression node, Void context)
//        {
//            RowExpression left = process(node.getLeft(), context);
//            RowExpression right = process(node.getRight(), context);
//
//            return call(
//                    node.getOperator().name(),
//                    functionResolution.comparisonFunction(node.getOperator(), left.getType(), right.getType()),
//                    BooleanType.INSTANCE,
//                    left,
//                    right);
//        }
//
//        @Override
//        public RowExpression visitFunctionCall(FunctionCall node, Void context)
//        {
//            List<RowExpression> arguments = node.getArguments().stream()
//                    .map(value -> process(value, context))
//                    .collect(toImmutableList());
//
//            List<TypeSignatureProvider> argumentTypes = arguments.stream()
//                    .map(RowExpression::getType)
//                    .map(Type::getTypeSignature)
//                    .map(TypeSignatureProvider::new)
//                    .collect(toImmutableList());
//
//            return call(node.getName().toString(),
//                    functionAndTypeManager.resolveFunction(
//                            Optional.of(sessionFunctions),
//                            transactionId,
//                            qualifyObjectName(node.getName()),
//                            argumentTypes),
//                    getType(node),
//                    arguments);
//        }
//
//        @Override
//        public RowExpression visitSymbolReference(SymbolReference node, Void context)
//        {
//            VariableReferenceExpression variable = new VariableReferenceExpression(node.getName(), getType(node));
//            Integer channel = layout.get(variable);
//            if (channel != null) {
//                return field(channel, variable.getType());
//            }
//
//            return variable;
//        }
//
//        @Override
//        public RowExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
//        {
//            RowExpression left = process(node.getLeft(), context);
//            RowExpression right = process(node.getRight(), context);
//
//            return call(
//                    node.getOperator().name(),
//                    functionResolution.arithmeticFunction(node.getOperator(), left.getType(), right.getType()),
//                    getType(node),
//                    left,
//                    right);
//        }
//
//        @Override
//        public RowExpression visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
//        {
//            RowExpression expression = process(node.getValue(), context);
//
//            switch (node.getSign()) {
//                case PLUS:
//                    return expression;
//                case MINUS:
//                    return call(
//                            NEGATION.name(),
//                            functionAndTypeManager.resolveOperator(NEGATION, fromTypes(expression.getType())),
//                            getType(node),
//                            expression);
//            }
//
//            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
//        }

        @Override
        public RowExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            Form form;
            switch (node.getOperator()) {
                case AND:
                    form = AND;
                    break;
                case OR:
                    form = OR;
                    break;
                default:
                    throw new IllegalStateException("Unknown logical operator: " + node.getOperator());
            }
            return specialForm(form, BooleanType.INSTANCE, List.of(process(node.getLeft(), context), process(node.getRight(), context)));
        }
//
//        @Override
//        public RowExpression visitCast(Cast node, Void context)
//        {
//            RowExpression value = process(node.getExpression(), context);
//
//            if (node.isSafe()) {
//                return call(TRY_CAST.name(), functionAndTypeManager.lookupCast(TRY_CAST, value.getType().getTypeSignature(), getType(node).getTypeSignature()), getType(node), value);
//            }
//
//            return call(CAST.name(), functionAndTypeManager.lookupCast(CAST, value.getType().getTypeSignature(), getType(node).getTypeSignature()), getType(node), value);
//        }
//
//        @Override
//        public RowExpression visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
//        {
//            return buildSwitch(process(node.getOperand(), context), node.getWhenClauses(), node.getDefaultValue(), getType(node), context);
//        }
//
//        private RowExpression buildSwitch(RowExpression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue, Type returnType, Void context)
//        {
//            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
//
//            arguments.add(operand);
//
//            for (WhenClause clause : whenClauses) {
//                arguments.add(specialForm(
//                        WHEN,
//                        getType(clause.getResult()),
//                        process(clause.getOperand(), context),
//                        process(clause.getResult(), context)));
//            }
//
//            arguments.add(defaultValue
//                    .map((value) -> process(value, context))
//                    .orElse(constantNull(returnType)));
//
//            return specialForm(SWITCH, returnType, arguments.build());
//        }
//
//        @Override
//        public RowExpression visitDereferenceExpression(DereferenceExpression node, Void context)
//        {
//            Type returnType = getType(node);
//            Optional<Object> maybeEnumLiteral = tryResolveEnumLiteral(node, returnType);
//            if (maybeEnumLiteral.isPresent()) {
//                return constant(maybeEnumLiteral.get(), returnType);
//            }
//
//            RowType rowType = (RowType) getType(node.getBase());
//            String fieldName = node.getField().getValue();
//            List<Field> fields = rowType.getFields();
//            int index = -1;
//            for (int i = 0; i < fields.size(); i++) {
//                Field field = fields.get(i);
//                if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName)) {
//                    checkArgument(index < 0, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
//                    index = i;
//                }
//            }
//
//            if (sqlFunctionProperties.isLegacyRowFieldOrdinalAccessEnabled() && index < 0) {
//                OptionalInt rowIndex = parseAnonymousRowFieldOrdinalAccess(fieldName, fields);
//                if (rowIndex.isPresent()) {
//                    index = rowIndex.getAsInt();
//                }
//            }
//
//            checkState(index >= 0, "could not find field name: %s", node.getField());
//            return specialForm(DEREFERENCE, returnType, process(node.getBase(), context), constant((long) index, INTEGER));
//        }
//
//        private RowExpression buildEquals(RowExpression lhs, RowExpression rhs)
//        {
//            return call(
//                    EQUAL.getOperator(),
//                    functionResolution.comparisonFunction(ComparisonExpression.Operator.EQUAL, lhs.getType(), rhs.getType()),
//                    BOOLEAN,
//                    lhs,
//                    rhs);
//        }
//
//        @Override
//        public RowExpression visitInPredicate(InPredicate node, Void context)
//        {
//            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
//            RowExpression value = process(node.getValue(), context);
//            InListExpression values = (InListExpression) node.getValueList();
//
//            if (values.getValues().size() == 1) {
//                return buildEquals(value, process(values.getValues().get(0), context));
//            }
//
//            arguments.add(value);
//            for (Expression inValue : values.getValues()) {
//                arguments.add(process(inValue, context));
//            }
//
//            return specialForm(IN, BOOLEAN, arguments.build());
//        }
//
//        @Override
//        public RowExpression visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
//        {
//            RowExpression expression = process(node.getValue(), context);
//
//            return call(
//                    "not",
//                    functionResolution.notFunction(),
//                    BOOLEAN,
//                    specialForm(IS_NULL, BOOLEAN, ImmutableList.of(expression)));
//        }
//
//        @Override
//        public RowExpression visitIsNullPredicate(IsNullPredicate node, Void context)
//        {
//            RowExpression expression = process(node.getValue(), context);
//
//            return specialForm(IS_NULL, BOOLEAN, expression);
//        }
//
//        @Override
//        public RowExpression visitNotExpression(NotExpression node, Void context)
//        {
//            return call("not", functionResolution.notFunction(), BOOLEAN, process(node.getValue(), context));
//        }
//
//        @Override
//        public RowExpression visitBetweenPredicate(BetweenPredicate node, Void context)
//        {
//            RowExpression value = process(node.getValue(), context);
//            RowExpression min = process(node.getMin(), context);
//            RowExpression max = process(node.getMax(), context);
//
//            return call(
//                    BETWEEN.name(),
//                    functionAndTypeManager.resolveOperator(BETWEEN, fromTypes(value.getType(), min.getType(), max.getType())),
//                    BOOLEAN,
//                    value,
//                    min,
//                    max);
//        }
//
//        @Override
//        public RowExpression visitLikePredicate(LikePredicate node, Void context)
//        {
//            RowExpression value = process(node.getValue(), context);
//            RowExpression pattern = process(node.getPattern(), context);
//
//            if (node.getEscape().isPresent()) {
//                RowExpression escape = process(node.getEscape().get(), context);
//                return likeFunctionCall(value, call("LIKE_PATTERN", functionResolution.likePatternFunction(), LIKE_PATTERN, pattern, escape));
//            }
//
//            return likeFunctionCall(value, call(CAST.name(), functionAndTypeManager.lookupCast(CAST, VARCHAR.getTypeSignature(), LIKE_PATTERN.getTypeSignature()), LIKE_PATTERN, pattern));
//        }
//
//        private RowExpression likeFunctionCall(RowExpression value, RowExpression pattern)
//        {
//            if (value.getType() instanceof VarcharType) {
//                return call("LIKE", functionResolution.likeVarcharFunction(), BOOLEAN, value, pattern);
//            }
//
//            checkState(value.getType() instanceof CharType, "LIKE value type is neither VARCHAR or CHAR");
//            return call("LIKE", functionResolution.likeCharFunction(value.getType()), BOOLEAN, value, pattern);
//        }

//        @Override
//        public RowExpression visitArrayConstructor(ArrayConstructor node, Void context)
//        {
//            List<RowExpression> arguments = node.getValues().stream()
//                    .map(value -> process(value, context))
//                    .collect(toImmutableList());
//            List<Type> argumentTypes = arguments.stream()
//                    .map(RowExpression::getType)
//                    .collect(toImmutableList());
//            return call("ARRAY", functionResolution.arrayConstructor(argumentTypes), getType(node), arguments);
//        }
//
//        @Override
//        public RowExpression visitRow(Row node, Void context)
//        {
//            List<RowExpression> arguments = node.getItems().stream()
//                    .map(value -> process(value, context))
//                    .collect(toImmutableList());
//            Type returnType = getType(node);
//            return specialForm(ROW_CONSTRUCTOR, returnType, arguments);
//        }
    }
}

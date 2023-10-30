package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.inference.SchemaBuilder.ArgCombination;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.ArgumentParameter;
import com.datasqrl.graphql.server.Model.FixedArgument;
import com.datasqrl.graphql.server.Model.JdbcParameterHandler;
import com.datasqrl.graphql.server.Model.QueryBase;
import com.datasqrl.graphql.server.Model.SourceParameter;
import com.datasqrl.graphql.server.Model.VariableArgument;
import com.datasqrl.graphql.util.ApiQueryBase;
import com.datasqrl.graphql.util.PagedApiQueryBase;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import graphql.language.IntValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

public class QueryBuilderHelper {


  private final QueryPlanner queryPlanner;
  private final SqlOperator operator;
  private final RelBuilder relBuilder;
  private final RexBuilder rexBuilder;
  private final RelDataType returnType;
  private final String nameId;
  private final APIConnectorManager apiManager;
  List<Argument> graphqlArguments = new ArrayList<>();
  // Parameter handler and operands should be
  List<Pair<RexNode, JdbcParameterHandler>> parameterHandler = new ArrayList<>();
  List<RexNode> extraFilters = new ArrayList<>();
  private boolean limitOffsetFlag = false;
  private Optional<ArgCombination> limit;
  private Optional<ArgCombination> offset;

  public QueryBuilderHelper(QueryPlanner queryPlanner, SqlOperator operator, RelBuilder relBuilder, RelDataType returnType,
      String nameId, APIConnectorManager apiManager) {
    this.queryPlanner = queryPlanner;
    this.operator = operator;
    this.relBuilder = relBuilder;
    this.rexBuilder = relBuilder.getRexBuilder();
    this.returnType = returnType;
    this.nameId = nameId;
    this.apiManager = apiManager;
  }

  public void filter(String name, RelDataType type) {
    RexDynamicParam rexDynamicParam = makeArgumentDynamicParam(name, type);

    VariableArgument variableArgument = new VariableArgument(name, null);
    graphqlArguments.add(variableArgument);

    RexInputRef inputRef = getColumnRef(name);
    extraFilters.add(relBuilder.equals(inputRef, rexDynamicParam));
  }

  private RexInputRef getColumnRef(String name) {
    Optional<RelDataTypeField> field = relBuilder.peek().getRowType().getFieldList().stream()
        .filter(f -> f.getName().equalsIgnoreCase(name))
        .findAny();

    RexInputRef ref = relBuilder.getRexBuilder()
        .makeInputRef(
            field.get().getType(),
            field.get().getIndex());
    return ref;
  }

  private RexDynamicParam makeArgumentDynamicParam(String name, RelDataType type) {
    ArgumentParameter argumentParameter = new ArgumentParameter(name);

    RexDynamicParam rexDynamicParam = rexBuilder.makeDynamicParam(type, parameterHandler.size());
    parameterHandler.add(Pair.of(rexDynamicParam, argumentParameter));
    return rexDynamicParam;
  }

  public void scan(SqlUserDefinedTableFunction operator) {
    Preconditions.checkState(parameterHandler.size() == operator.getFunction().getParameters().size());
    List<RexNode> operands = parameterHandler.stream()
        .map(Pair::getLeft)
        .collect(Collectors.toList());

    relBuilder.functionScan(operator, 0, operands);
  }

  public void addInternalOperand(String name, RelDataType type) {
    //Add an operand and a parameter handler
    makeSourceDynamicParam(name, type);
  }

  private RexDynamicParam makeSourceDynamicParam(String name, RelDataType type) {
    RexDynamicParam rexDynamicParam = rexBuilder.makeDynamicParam(type,
        parameterHandler.size());//todo check casting rules
    parameterHandler.add(Pair.of(rexDynamicParam, new SourceParameter(name)));
    return rexDynamicParam;
  }

  public void addLiteralOperand(String name, RelDataType type, Object value) {
    FixedArgument fixedArgument = new FixedArgument(name, value);//todo casting
    graphqlArguments.add(fixedArgument);
  }

  public RexDynamicParam addVariableOperand(String name, RelDataType type) {
    RexDynamicParam rexDynamicParam = makeArgumentDynamicParam(name, type);

    VariableArgument variableArgument = new VariableArgument(name, null);
    graphqlArguments.add(variableArgument);
    return rexDynamicParam;
  }

  public void limitOffset(Optional<ArgCombination> limit, Optional<ArgCombination> offset,
      int numPrimaryKeys) {
    this.limit = limit;
    this.offset = offset;
    if (limit.isPresent() || offset.isPresent()) {
      this.limitOffsetFlag = true;
      if (limit.isPresent()) {
        if (limit.get().getDefaultValue().isEmpty()) {
          createVariableForLimitOffset(LIMIT);
        } else {
          createLiteralForLimitOffset(LIMIT);
        }
      }

      if (offset.isPresent()) {
        if (offset.get().getDefaultValue().isEmpty()) {
          createVariableForLimitOffset(OFFSET);
        } else {
          createLiteralForLimitOffset(OFFSET);
        }
      }
    }
  }

  // Standalone variable without dynamic params since server will rewrite it
  private void createVariableForLimitOffset(String name) {
    VariableArgument argument = new VariableArgument(name, null);
    graphqlArguments.add(argument);
  }
  private void createLiteralForLimitOffset(String name) {
    FixedArgument argument = new FixedArgument(name, null);
    graphqlArguments.add(argument);
  }

  /**
   * TODO: Cannot use this strategy because sorting happens in dag planning and no index is created
   *  so row num would very expensive.
   */
  public void limitOffsetRowNum(Optional<ArgCombination> limit, Optional<ArgCombination> offset,
      int numPrimaryKeys) {
    if (limit.isPresent() || offset.isPresent()) {
      RelDataType type = relBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);

      List<RelDataTypeField> partitionKeysFields = IntStream.range(0, numPrimaryKeys)
          .mapToObj(i -> relBuilder.peek().getRowType().getFieldList().get(i))
          .collect(Collectors.toList());

      List<RelFieldCollation> relFieldCollations = partitionKeysFields.stream()
          .map(f -> new RelFieldCollation(f.getIndex(), Direction.DESCENDING, NullDirection.LAST))
          .collect(Collectors.toList());
      RelCollation collation = RelCollations.of(relFieldCollations);

      Optional<Integer> limitValue = limit.flatMap(ArgCombination::getDefaultValue)
          .map(i -> (IntValue) i)
          .map(i -> i.getValue().intValue());

      Optional<Integer> offsetValue = offset.flatMap(ArgCombination::getDefaultValue)
          .map(i -> (IntValue) i)
          .map(i -> i.getValue().intValue());

      if (offset.isPresent() && limit.isPresent()) {
        if (limitValue.isPresent() && offsetValue.isPresent()) { //both are scalars
          addArgumentLiteralFilter(LIMIT, limitValue.get());
          addArgumentLiteralFilter(OFFSET, offsetValue.get());
          relBuilder
              .limit(offsetValue.get(), limitValue.get());
        } else if (limitValue.isPresent()) { //offset is a variable
          addArgumentLiteralFilter(LIMIT, limitValue.get());
          RexDynamicParam param = addVariableOperand(OFFSET, type);

          RexInputRef rowNum = createRowNum(partitionKeysFields, relBuilder);
          relBuilder
              .filter(relBuilder.call(SqlStdOperatorTable.GREATER_THAN, rowNum, param))
              .limit(0, limitValue.get());
        } else if (offsetValue.isPresent()) { //limit is a variable
          RexDynamicParam param = addVariableOperand(LIMIT, type);
          addArgumentLiteralFilter(OFFSET, offset.get());

          RexInputRef rowNum = createRowNum(partitionKeysFields, relBuilder);
          // between offset + 1 and limit + offset
          relBuilder
              .filter(relBuilder.call(SqlStdOperatorTable.BETWEEN, rowNum,
                  relBuilder.literal(offsetValue.get() + 1),
                  relBuilder.call(SqlStdOperatorTable.PLUS, relBuilder.literal(offsetValue.get()),
                      param)));
        } else { //both are variables
          RexDynamicParam limitParam = addVariableOperand(LIMIT, type);
          RexDynamicParam offsetParam = addVariableOperand(OFFSET, type);

          RexInputRef rowNum = createRowNum(partitionKeysFields, relBuilder);
          //
          // between offset + 1 and limit + offset
          relBuilder
              .filter(relBuilder.call(SqlStdOperatorTable.BETWEEN, rowNum,
                  relBuilder.call(SqlStdOperatorTable.PLUS, offsetParam,
                      relBuilder.literal(1)),
                  relBuilder.call(SqlStdOperatorTable.PLUS, offsetParam,
                      limitParam)));
        }
      } else if (offset.isPresent()) {
        if (offsetValue.isPresent()) {
          relBuilder
              .limit(offsetValue.get(), 0);
        } else {
          RexDynamicParam offsetParam = addVariableOperand(OFFSET, type);

          RexInputRef rowNum = createRowNum(partitionKeysFields, relBuilder);

          relBuilder
              .filter(relBuilder.call(SqlStdOperatorTable.GREATER_THAN, rowNum, offsetParam));
        }
      } else if (limit.isPresent()) {
        if (limitValue.isPresent()) {
          relBuilder
              .limit(0, limitValue.get());
        } else {
          RexDynamicParam limitParam = addVariableOperand(LIMIT, type);

          RexInputRef rowNum = createRowNum(partitionKeysFields, relBuilder);

          relBuilder
              .filter(relBuilder.call(SqlStdOperatorTable.GREATER_THAN, rowNum, limitParam));
        }
      } else {
        throw new RuntimeException("unexpected");
      }
    }
  }

  private void addArgumentLiteralFilter(String name, Object o) {
    FixedArgument fixedArgument = new FixedArgument(name, o);
    this.graphqlArguments.add(fixedArgument);
  }

  public void applyExtraFilters() {
    if (extraFilters.isEmpty()) {
      return;
    }
    relBuilder.filter(extraFilters);
  }

  public Model.ArgumentSet build() {

    RelNode rel = relBuilder.build();

    RelNode expanded = queryPlanner.expandMacros(rel);

    APIQuery query = new APIQuery(nameId, expanded);
    apiManager.addQuery(query);
    List<JdbcParameterHandler> handlers = this.parameterHandler.stream()
        .map(p -> p.getRight())
        .collect(Collectors.toList());

    QueryBase queryBase;

    if (limitOffsetFlag) {
      queryBase = PagedApiQueryBase.builder()
          .parameters(handlers)
          .relNode(expanded)
          .query(query)
          .build();
    } else {
      queryBase = ApiQueryBase.builder()
          .parameters(handlers)
          .relNode(expanded)
          .query(query)
          .build();
    }

    Model.ArgumentSet matchSet = Model.ArgumentSet.builder()
        .arguments(this.graphqlArguments)
        .query(queryBase)
        .build();

    return matchSet;
  }


  private RexInputRef createRowNum(List<RelDataTypeField> relFields, RelBuilder relBuilder) {

    List<RexFieldCollation> collations = relFields.stream()
        .map(f -> new RexFieldCollation(RexInputRef.of(f.getIndex(), relFields),
            Set.of(SqlKind.NULLS_LAST)))
        .collect(Collectors.toList());

    List<RexNode> partition = relFields.stream()
        .map(f -> RexInputRef.of(f.getIndex(), relFields))
        .collect(Collectors.toList());
    SqrlRexUtil rexUtil = new SqrlRexUtil(relBuilder.getTypeFactory());
    RexNode rowFunction = rexUtil.createRowFunction(SqlStdOperatorTable.ROW_NUMBER, List.of(),
        List.of());

    relBuilder.projectPlus(rowFunction);
    RelDataType row = relBuilder.peek().getRowType();
    RelDataTypeField rowNumField = row.getFieldList().get(row.getFieldList().size() - 1);

    return RexInputRef.of(rowNumField.getIndex(), row.getFieldList());
  }
}
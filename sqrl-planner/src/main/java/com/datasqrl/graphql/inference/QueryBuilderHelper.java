package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.inference.GraphqlQueryGenerator.ArgCombination;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.FixedArgument;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcParameterHandler;
import com.datasqrl.graphql.server.RootGraphqlModel.SourceParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.VariableArgument;
import com.datasqrl.plan.queries.APIQuery;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

public class QueryBuilderHelper {


  private final QueryPlanner queryPlanner;
  private final RelBuilder relBuilder;
  private final RexBuilder rexBuilder;
  private final String nameId;
  List<Argument> graphqlArguments = new ArrayList<>();
  List<Pair<RexNode, JdbcParameterHandler>> parameterHandler = new ArrayList<>();
  List<RexNode> extraFilters = new ArrayList<>();
  private boolean limitOffsetFlag = false;

  public QueryBuilderHelper(QueryPlanner queryPlanner, RelBuilder relBuilder,
      String nameId) {
    this.queryPlanner = queryPlanner;
    this.relBuilder = relBuilder;
    this.rexBuilder = relBuilder.getRexBuilder();
    this.nameId = nameId;
  }

  public void filter(String name, RelDataType type) {
    RexDynamicParam rexDynamicParam = makeArgumentDynamicParam(name, type);

    VariableArgument variableArgument = new VariableArgument(name, null);
    graphqlArguments.add(variableArgument);

    RexInputRef inputRef = getColumnRef(name, type);
    extraFilters.add(relBuilder.equals(inputRef, rexDynamicParam));
  }

  private RexInputRef getColumnRef(String name, RelDataType type) {
    RelDataType rowType = relBuilder.peek().getRowType();
    int index = queryPlanner.getCatalogReader().nameMatcher()
        .indexOf(rowType.getFieldNames(), name);
    if (index == -1) {
      throw new RuntimeException("Could not find filter for graphql column: " + name);
    }

    RelDataTypeField field = rowType.getFieldList().get(index);

    // Todo: check for casting between type and field.getType
    return relBuilder.getRexBuilder()
        .makeInputRef(
            field.getType(),
            field.getIndex());
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

  public RexDynamicParam addVariableOperand(String name, RelDataType type) {
    RexDynamicParam rexDynamicParam = makeArgumentDynamicParam(name, type);

    VariableArgument variableArgument = new VariableArgument(name, null);
    graphqlArguments.add(variableArgument);
    return rexDynamicParam;
  }

  public void limitOffset(Optional<ArgCombination> limit, Optional<ArgCombination> offset) {
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

  public void applyExtraFilters() {
    if (extraFilters.isEmpty()) {
      return;
    }
    relBuilder.filter(extraFilters);
  }

  public APIQuery build(NamePath path) {
    RelNode rel = relBuilder.build();

    RelNode expanded = queryPlanner.expandMacros(rel);
    List<JdbcParameterHandler> parameters = this.parameterHandler.stream()
        .map(Pair::getRight)
        .collect(Collectors.toList());
    return new APIQuery(nameId, path, expanded, parameters, this.graphqlArguments, limitOffsetFlag);
  }
}
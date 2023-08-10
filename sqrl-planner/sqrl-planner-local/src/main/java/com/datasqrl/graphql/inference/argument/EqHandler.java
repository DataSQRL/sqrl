/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference.argument;

import com.datasqrl.graphql.inference.ArgumentSet;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.ArgumentParameter;
import com.datasqrl.graphql.server.Model.VariableArgument;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.schema.Column;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

public class EqHandler implements ArgumentHandler {

  @Override
  public Set<ArgumentSet> accept(ArgumentHandlerContextV1 context) {
    //if optional, assure we re-emit all args
    Set<ArgumentSet> set = new HashSet<>(context.getArgumentSet());
    RexBuilder rexBuilder = context.getRelBuilder().getRexBuilder();
    for (ArgumentSet args : context.getArgumentSet()) {
      RelBuilder relBuilder = context.getRelBuilder();
      relBuilder.push(args.getRelNode());

      Column vtField = (Column) context.getTable()
          .getField(Name.system(context.getArg().getName()))
          .orElseThrow(
              () -> new RuntimeException("Could not find field: " + context.getArg().getName()));
      RelDataTypeField field = relBuilder.peek().getRowType()
          .getField(vtField.getVtName().getCanonical(), false, false);
      RexDynamicParam dynamicParam = rexBuilder.makeDynamicParam(field.getType(),
          context.getSourceHandlers().size() + args.getArgumentHandlers().size());
      RelNode rel = relBuilder.filter(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(relBuilder.peek(), field.getIndex()), dynamicParam)).build();

      Set<Argument> newHandlers = new LinkedHashSet<>(args.getArgumentHandlers());
      newHandlers.add(VariableArgument.builder().path(context.getArg().getName()).build());

      List<ArgumentParameter> parameters = new ArrayList<>(args.getArgumentParameters());
      parameters.add(ArgumentParameter.builder().path(context.getArg().getName()).build());

      set.add(new ArgumentSet(rel, newHandlers, parameters, args.isLimitOffsetFlag()));
    }

    //if optional: add an option to the arg permutation list
    return set;
  }

  @Override
  public boolean canHandle(ArgumentHandlerContextV1 context) {
    return context.getTable().getField(Name.system(context.getArg().getName())).isPresent();
  }
}
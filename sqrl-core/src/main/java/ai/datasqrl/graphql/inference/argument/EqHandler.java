package ai.datasqrl.graphql.inference.argument;

import ai.datasqrl.graphql.inference.ArgumentSet;
import ai.datasqrl.graphql.server.Model.Argument;
import ai.datasqrl.graphql.server.Model.VariableArgument;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Column;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
      Set<ArgumentSet> set = new HashSet<>(context.getRelAndArgs());
      RexBuilder rexBuilder = context.getRelBuilder().getRexBuilder();
      for (ArgumentSet args : context.getRelAndArgs()) {
        RelBuilder relBuilder = context.getRelBuilder();
        relBuilder.push(args.getRelNode());

        Column shadowedField = (Column)context.getTable().getField(Name.system(context.getArg().getName()))
            .orElseThrow(()->new RuntimeException("Could not find field: " + context.getArg().getName()));
        RelDataTypeField field = relBuilder.peek().getRowType()
            .getField(shadowedField.getShadowedName().getCanonical(), false, false);
        RexDynamicParam dynamicParam = rexBuilder.makeDynamicParam(field.getType(),
            context.getSourceHandlers().size() + args.getArgumentHandlers().size());
        RelNode rel = relBuilder.filter(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(relBuilder.peek(), field.getIndex()), dynamicParam)).build();

        Set<Argument> newHandlers = new LinkedHashSet<>(args.getArgumentHandlers());
        newHandlers.add(VariableArgument.builder().path(context.getArg().getName()).build());

        set.add(new ArgumentSet(rel, newHandlers, args.isLimitOffsetFlag()));
      }

      //if optional: add an option to the arg permutation list
      return set;
    }

    @Override
    public boolean canHandle(ArgumentHandlerContextV1 context) {
      return context.getTable().getField(Name.system(context.getArg().getName())).isPresent();
    }
  }
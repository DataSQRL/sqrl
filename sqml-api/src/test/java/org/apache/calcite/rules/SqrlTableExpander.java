package org.apache.calcite.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.CalciteSqrlTable;
import org.apache.calcite.rules.SqrlTableExpander.Config;
import org.immutables.value.Value;

public class SqrlTableExpander
      extends RelRule<Config>
      implements TransformationRule {


  public SqrlTableExpander(Config config) {
      super(config);
  }

    @Override public void onMatch(RelOptRuleCall call) {
      LogicalTableScan tableScan = call.rel(0);
      CalciteSqrlTable table = tableScan.getTable().unwrap(CalciteSqrlTable.class);
//      SqrlTable3 sqrlTable = schema.walkTable(table.getQualifiedName().get(table.getQualifiedName().size() - 1));

      System.out.println();
      System.out.println();
//      call.transformTo(call.rel(0).getInput(0));
    }
//  int Foo = 0;
//  int Foo() {return 0;}
    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
      Config DEFAULT = EMPTY
          .withOperandSupplier(b ->
              b.operand(TableScan.class).anyInputs())
          .as(Config.class);

      @Override default SqrlTableExpander toRule() {
        return new SqrlTableExpander(this);
      }
    }
  }
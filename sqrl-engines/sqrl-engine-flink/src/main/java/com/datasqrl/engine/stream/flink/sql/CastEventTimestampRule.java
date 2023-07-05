package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.hints.WatermarkHint;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

/**
 * " Please select the column that should be used as the event-time timestamp for the table sink by casting all other
 *   columns to regular TIMESTAMP or TIMESTAMP_LTZ."
 */
public class CastEventTimestampRule extends RelRule<CastEventTimestampRule.Config>
    implements TransformationRule {

  protected CastEventTimestampRule() {
    super(CastEventTimestampRule.Config.DEFAULT);
  }

  private boolean hasMatched = false;

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject project = call.rel(0);

    if (hasMatched) {
      return;
    }
    hasMatched = true;

    RelBuilder relBuilder = call.builder();

    long numTimestamps = project.getRowType().getFieldList().stream()
            .filter(f->f.getType().getSqlTypeName() == TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .count();

    int tsIndex = getTimestampIndex(project.getRowType().getFieldList());
    if (tsIndex == -1 || numTimestamps < 2) {
      return;
    }
    //Cast all other timestamps
    List<RexNode> rex = new ArrayList<>();
    List<String> names = new ArrayList<>();
    for (int i = 0; i < project.getRowType().getFieldList().size(); i++) {
      RelDataTypeField field = project.getRowType().getFieldList().get(i);
      RexNode node = project.getProjects().get(i);
      if (field.getType().getSqlTypeName() == TIMESTAMP_WITH_LOCAL_TIME_ZONE
              && i != tsIndex) {
        RexNode casted = relBuilder.getRexBuilder().makeCast(field.getType(), node, true);
        rex.add(casted);
      } else {
        rex.add(node);
      }
      names.add(field.getName());
    }

    RelNode relNode = relBuilder.push(project.getInput())
            .project(rex, names)
            .hints(project.getHints())
            .build();

    call.transformTo(relNode);
  }

  private int getTimestampIndex(List<RelDataTypeField> fieldList) {
    //Look for in order:
    // the latest 'timestamp'*
    // a '_source_time'

    for (int i = fieldList.size() - 1; i >= 0; i--) {
      if (fieldList.get(i).getType().getSqlTypeName() == TIMESTAMP_WITH_LOCAL_TIME_ZONE
              && fieldList.get(i).getName().startsWith("timestamp")) {
        return i;
      }
    }
    for (int i = fieldList.size() - 1; i >= 0; i--) {
      if (fieldList.get(i).getType().getSqlTypeName() == TIMESTAMP_WITH_LOCAL_TIME_ZONE
              && fieldList.get(i).getName().equalsIgnoreCase("_source_time")) {
        return i;
      }
    }

    return -1;
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    CastEventTimestampRule.Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class).anyInputs())
        .withDescription("CastEventTimestampRule")
        .as(CastEventTimestampRule.Config.class);

    @Override default CastEventTimestampRule toRule() {
      return new CastEventTimestampRule();
    }

    /** Whether to include outer joins, default false. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isIncludeOuter();

    /** Sets {@link #isIncludeOuter()}. */
    CastEventTimestampRule.Config withIncludeOuter(boolean includeOuter);
  }
}

package com.datasqrl.util;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

/**
 * Methods copied from Calcite's {@link RelWriterImpl} to add hints to the output
 */
public class TestRelWriter extends RelWriterImpl {

  public boolean withHints = true;

  public TestRelWriter(PrintWriter pw) {
    super(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
  }

  public static String explain(RelNode rel) {
    StringWriter sw = new StringWriter();
    TestRelWriter writer = new TestRelWriter(new PrintWriter(sw));
    rel.explain(writer);
    return sw.toString();
  }

  /**
   * Copied verbatim from {@link RelWriterImpl} and added the marked section of code to print hints
   *
   * @param rel
   * @param values
   */
  @Override
  protected void explain_(RelNode rel,
      List<Pair<String, Object>> values) {
    List<RelNode> inputs = rel.getInputs();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    if (!mq.isVisibleInExplain(rel, detailLevel)) {
      // render children in place of this, at same level
      explainInputs(inputs);
      return;
    }

    StringBuilder s = new StringBuilder();
    spacer.spaces(s);
    if (withIdPrefix) {
      s.append(rel.getId()).append(":");
    }
    s.append(rel.getRelTypeName());
    if (detailLevel != SqlExplainLevel.NO_ATTRIBUTES) {
      int j = 0;
      for (Pair<String, Object> value : values) {
        if (value.right instanceof RelNode) {
          continue;
        }
        if (j++ == 0) {
          s.append("(");
        } else {
          s.append(", ");
        }
        s.append(value.left)
            .append("=[")
            .append(value.right)
            .append("]");
      }
      if (j > 0) {
        s.append(")");
      }
    }
    //===== Added this code to print hints ========
    if (withHints && rel instanceof Hintable) {
      int j = 0;
      for (RelHint hint : ((Hintable) rel).getHints()) {
        if (j++ == 0) {
          s.append(" hints[");
        } else {
          s.append(", ");
        }
        s.append(hint.hintName);
        if (!hint.listOptions.isEmpty()) {
          s.append(" options:").append(hint.listOptions);
        }
      }
      if (j > 0) {
        s.append("]");
      }
    }
    //===== end of added code =====

    switch (detailLevel) {
      case ALL_ATTRIBUTES:
        s.append(": rowcount = ")
            .append(mq.getRowCount(rel))
            .append(", cumulative cost = ")
            .append(mq.getCumulativeCost(rel));
    }
    pw.println(s);
    spacer.add(2);
    explainInputs(inputs);
    spacer.subtract(2);

  }

  /**
   * Copied verbatim from {@link RelWriterImpl}
   *
   * @param inputs
   */
  private void explainInputs(List<RelNode> inputs) {
    for (RelNode input : inputs) {
      input.explain(this);
    }
  }
}

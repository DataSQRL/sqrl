package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.Assignment;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.StreamAssignment;
import org.apache.calcite.sql.StreamType;
import org.apache.calcite.tools.RelBuilder;

public abstract class AbstractQueryStatementResolver extends AbstractStatementResolver {


  private final CalciteTableFactory tableFactory;

  protected AbstractQueryStatementResolver(ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner, CalciteTableFactory tableFactory) {
    super(errors, nameCanonicalizer, planner);
    this.tableFactory = tableFactory;
  }

  public void resolve(Assignment statement, Namespace ns) {
    SqlNode sqlNode = transpile(statement, ns);
    RelNode relNode = plan(sqlNode);

    AnnotatedLP prel = convert(planner, relNode, ns, getPostProcessor(ns, relNode), statement instanceof StreamAssignment, statement.getHints());

    Optional<StreamType> type =
        Optional.of(statement).filter(f->f instanceof StreamAssignment).map(f->((StreamAssignment)f).getType());
    NamespaceObject table = tableFactory.createTable(planner, ns, statement.getNamePath(), prel, type, getContext(ns, statement.getNamePath()));

    ns.addNsObject(table);
  }

  public abstract Function<AnnotatedLP, AnnotatedLP> getPostProcessor(Namespace ns, RelNode relNode);

  //Post-process the AnnotatedLP to account for unique statement kinds
  protected AnnotatedLP postProcessAnnotatedLP(RelBuilder relBuilder, AnnotatedLP prel, List<String> fieldNames) {
    return prel.postProcess(relBuilder, fieldNames);
  }

  //Post-process the AnnotatedLP to account for unique statement kinds
  protected AnnotatedLP postProcessStreamAnnotatedLP(RelBuilder relBuilder, AnnotatedLP prel, List<String> fieldNames) {
    return prel.postProcessStream(relBuilder, fieldNames);
  }
}

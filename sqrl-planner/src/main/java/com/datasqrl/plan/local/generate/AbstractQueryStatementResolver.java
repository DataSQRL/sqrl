package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.local.generate.SqrlStatementVisitor.SystemContext;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.Assignment;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.StreamAssignment;
import org.apache.calcite.sql.SubscriptionType;
import org.apache.calcite.tools.RelBuilder;

public abstract class AbstractQueryStatementResolver extends AbstractStatementResolver {

  public AbstractQueryStatementResolver(SystemContext systemContext) {
    super(systemContext);
  }

  public void resolve(Assignment statement, Namespace ns) {
    SqlNode sqlNode = transpile(statement, ns);
    RelNode relNode = plan(sqlNode, ns);

    AnnotatedLP prel = convert(relNode, ns, getPostProcessor(ns, relNode), statement instanceof StreamAssignment, statement.getHints());

    NsTableFactory tableFactory = new NsTableFactory(ns.tableFactory);

    Optional<SubscriptionType> type =
        Optional.of(statement).filter(f->f instanceof StreamAssignment).map(f->((StreamAssignment)f).getType());
    NamespaceObject table = tableFactory.createTable(ns, statement.getNamePath(), prel, type, getContext(ns, statement.getNamePath()));

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

package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.table.CalciteTableFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.Assignment;
import org.apache.calcite.sql.SqlNode;

public abstract class AbstractQueryStatementResolver extends AbstractStatementResolver {

  protected final CalciteTableFactory tableFactory;

  protected AbstractQueryStatementResolver(ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner, CalciteTableFactory tableFactory) {
    super(errors, nameCanonicalizer, planner);
    this.tableFactory = tableFactory;
  }

  protected boolean setOriginalFieldnames() {
    return true;
  }

  protected RelNode preprocessRelNode(RelNode relNode, Assignment statement) {
    return relNode;
  }

  public void resolve(Assignment statement, Namespace ns) {
    SqlNode sqlNode = transpile(statement, ns);
    RelNode relNode = plan(sqlNode);
    relNode = preprocessRelNode(relNode,statement);

    StatementSQRLConverter converter = new StatementSQRLConverter();
    LPAnalysis analyzedLP = converter.convert(planner, relNode, setOriginalFieldnames(), ns, statement.getHints(), errors);

    createTable(ns, statement, analyzedLP);
  }

  protected SqrlTableNamespaceObject createTableInternal(Namespace ns, Assignment statement, LPAnalysis analyzedLP) {
    return tableFactory.createTable(statement.getNamePath(), analyzedLP,
        getContext(ns, statement.getNamePath()));
  }

  protected void createTable(Namespace ns, Assignment statement, LPAnalysis analyzedLP) {
    SqrlTableNamespaceObject table = createTableInternal(ns, statement, analyzedLP);
    ns.addNsObject(table);
  }

}

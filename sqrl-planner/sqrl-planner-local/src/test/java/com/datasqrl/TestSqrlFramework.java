package com.datasqrl;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.SqrlPlanningTableFactory;
import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.TableConverter;
import com.datasqrl.plan.table.TableIdFactory;
import com.datasqrl.util.SqlNameUtil;
import com.google.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqrlStatement;

public class TestSqrlFramework extends SqrlFramework {

  private final ErrorCollector errors;

  public TestSqrlFramework() {
    super(SqrlRelMetadataProvider.INSTANCE,
        SqrlHintStrategyTable.getHintStrategyTable(), NameCanonicalizer.SYSTEM);
    this.errors = ErrorCollector.root();
  }

  public TestQueryPlanner getQueryPlanner(ModuleLoader moduleLoader) {
    return new TestQueryPlanner(this, super.getQueryPlanner(), moduleLoader);
  }

  public class TestQueryPlanner extends QueryPlanner {
    @Delegate
    QueryPlanner queryPlanner;
    private final ModuleLoader moduleLoader;

    public TestQueryPlanner(SqrlFramework framework, QueryPlanner queryPlanner,
        ModuleLoader moduleLoader) {
      super(framework);
      this.queryPlanner = queryPlanner;
      this.moduleLoader = moduleLoader;
    }

    public TestQueryPlanner planSqrl(String statement) {
      try {
        planSqrl(parseSqrlStatement(statement),
            new SqrlPlanningTableFactory(getFramework(), new SqlNameUtil(getFramework().getNameCanonicalizer()),
                new CalciteTableFactory(new TableIdFactory(getFramework().getTableNameToIdMap()),
                    new TableConverter(getFramework().getTypeFactory(), getFramework().getNameCanonicalizer()))), moduleLoader, errors);
      } catch (Exception e) {
        System.out.println(ErrorPrinter.prettyPrint(errors));
        throw e;
      }
      return this;
    }

    private SqrlStatement parseSqrlStatement(String statement) {
      //todo: pass generics to dialect
      return (SqrlStatement) ((ScriptNode)parse(Dialect.SQRL, statement)).getStatements().get(0);
    }
  }
}

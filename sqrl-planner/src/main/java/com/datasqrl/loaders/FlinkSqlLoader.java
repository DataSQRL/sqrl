package com.datasqrl.loaders;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.util.BaseFileUtil;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;

public class FlinkSqlLoader {

  private final SqrlFramework framework;
  private final ResourceResolver resourceResolver;
  private final CalciteTableFactory tableFactory;
  private final ModuleLoaderImpl moduleLoader;

  public FlinkSqlLoader(SqrlFramework framework, ResourceResolver resourceResolver, CalciteTableFactory tableFactory,
      ModuleLoaderImpl moduleLoader) {
    this.framework = framework;
    this.resourceResolver = resourceResolver;
    this.tableFactory = tableFactory;
    this.moduleLoader = moduleLoader;
  }

  public List<NamespaceObject> load(NamePath namePath) {
    List<NamespaceObject> objects = new ArrayList<>();

    if (namePath.isEmpty()) return objects;
    List<URI> uris = resourceResolver.loadPath(namePath.popLast());
    //look for namePath.getLast() .sql

    Optional<String> flinkSql = getFlinkSql(uris,namePath.getLast());
    if (flinkSql.isEmpty()) return objects;

    List<String> cmds = List.of(flinkSql.get().split("\n\n"));


    SqlConformance conformance =  FlinkSqlConformance.DEFAULT;
    Config config = SqlParser.config()
        .withParserFactory(FlinkSqlParserFactories.create(conformance))
        .withConformance(conformance)
        .withLex(Lex.JAVA)
        .withIdentifierMaxLength(256);

    Set<SqlNode> nonTables = new LinkedHashSet<>();

    for (String cmd : cmds) {
      try {
        cmd = cleanSql(cmd);
        if (cmd.isEmpty()) continue;
        SqlParser parser = SqlParser.create(cmd, config);
        SqlNode sqlNode = parser.parseStmt();

        if (sqlNode instanceof SqlCreateTable) {
          objects.add(new FlinkSqlNamespaceObject(framework, (SqlCreateTable)sqlNode,
              nonTables, tableFactory, NameCanonicalizer.SYSTEM, moduleLoader));
        } else {
          nonTables.add(sqlNode);
        }
      } catch (Exception e) {
        e.printStackTrace();
        return new ArrayList<>();
      }
    }

    return objects;
  }

  private String cleanSql(String cmd) {
    cmd = cmd.trim();
    if (cmd.endsWith(";")) return cmd.substring(0, cmd.length()-1);
    return cmd;
  }

  private Optional<String> getFlinkSql(List<URI> uris, Name last) {
    for (URI uri : uris) {

      if (ResourceResolver.getFileName(uri).toLowerCase()
          .equals(last.getCanonical() + ".sql")) {
        return Optional.of(BaseFileUtil.readFile(uri));
      }
    }

    return Optional.empty();
  }
}

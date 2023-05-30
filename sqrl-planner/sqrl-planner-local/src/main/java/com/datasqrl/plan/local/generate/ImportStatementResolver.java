package com.datasqrl.plan.local.generate;

import static com.datasqrl.error.ErrorLabel.GENERIC;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleMetadataPrinter;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.ImportDefinition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

public class ImportStatementResolver extends AbstractStatementResolver {

  private final ModuleLoader moduleLoader;

  protected ImportStatementResolver(ModuleLoader moduleLoader, ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner) {
    super(errors, nameCanonicalizer, planner);
    this.moduleLoader = moduleLoader;
  }

  public void resolve(ImportDefinition statement, Namespace ns) {
    NamePath path = statement.getNamePath();

    // Get the module specified in the import statement
    SqrlModule module = getModule(path);

    boolean loaded;
    if (isAllImport(path)) {
      checkState(statement.getAlias().isEmpty(),
          GENERIC, statement.getImportPath()::getParserPosition,
          () -> String.format("Alias not expected here: %s", path));

      checkState(statement.getTimestamp().isEmpty() && statement.getTimestampAlias().isEmpty(),
          GENERIC, statement.getImportPath()::getParserPosition,
          () -> String.format("Timestamp not expected here: %s", path));

      loaded = loadAllNamespaceObjects(module, ns);
    } else {
      // Get the namespace object specified in the import statement
      Optional<NamespaceObject> nsObject = getNamespaceObject(module, path);

      checkState(nsObject.isPresent(), GENERIC, statement.getImportPath()::getParserPosition,
          () -> String.format("Could not resolve import [%s] \n%s", path,
              ModuleMetadataPrinter.print(moduleLoader.getModuleMetadata(path))));

      // Add the namespace object to the current namespace
      Name objectName = getObjectName(path.getLast(), statement.getAlias());
      loaded = ns.addNsObject(objectName, nsObject.get());

      if (statement.getTimestamp().isPresent()) {
        addTimestampAsColumn(statement, ns, objectName);
      }
    }

    // Check if import loaded successfully
    checkState(loaded, GENERIC, statement.getImportPath()::getParserPosition,
        () -> String.format("Could not load import [%s]", path));
  }

  private void addTimestampAsColumn(ImportDefinition statement, Namespace ns, Name tableName) {

    SqlNode sqlNode = transpile(statement, ns);

    Preconditions.checkNotNull(sqlNode);
    RelNode relNode = plan(sqlNode);

    //if there is no timestamp alias, we call setTimestampColumn
    if (statement.getTimestampAlias().isEmpty()) {
      setTimestampColumn(statement, ns);
    } else {
      SqlIdentifier name = statement.getTimestampAlias().get();
      Optional<SQRLTable> table = resolveTable(ns, tableName.toNamePath(), false);
      Name name1 = Name.system(name.names.get(0));
      Preconditions.checkState(table.isPresent(), "Could not find table during import");
      table.ifPresent(t->t.addColumn(name1, relNode, true));
    }
  }

  private void setTimestampColumn(ImportDefinition importDefinition, Namespace ns) {
    Name tableName = getTableName(importDefinition);
    SQRLTable table = (SQRLTable) ns.getSchema().getTable(tableName.getCanonical(), false).getTable();
    ScriptRelationalTable baseTbl = getBaseTable(table);
    Preconditions.checkState(importDefinition.getTimestamp().isPresent(),
        ErrorCode.TIMESTAMP_COLUMN_EXPRESSION);
    SqlIdentifier identifier = getTimestampIdentifier(importDefinition);
    RelDataTypeField field = getFieldFromTable(baseTbl, identifier);
//    checkState(field != null,
//        ErrorCode.TIMESTAMP_COLUMN_MISSING, importDefinition.getTimestamp().get());
    baseTbl.getTimestamp().getCandidateByIndex(field.getIndex()).lockTimestamp();
  }

  private Name getTableName(ImportDefinition importDefinition) {
    return importDefinition.getAlias()
        .map(i -> Name.system(i.names.get(0)))
        .orElse(importDefinition.getNamePath().getLast());
  }

  private SqlIdentifier getTimestampIdentifier(ImportDefinition importDefinition) {
    return (SqlIdentifier) importDefinition.getTimestamp().get();
  }

  private RelDataTypeField getFieldFromTable(ScriptRelationalTable baseTbl, SqlIdentifier identifier) {
    return baseTbl.getRowType().getField(identifier.names.get(0), false, false);
  }

  private ScriptRelationalTable getBaseTable(SQRLTable table) {
    return ((VirtualRelationalTable.Root) table.getVt()).getBase();
  }
  private SqrlModule getModule(NamePath path) {
    return moduleLoader
        .getModule(path.popLast())
        .orElseThrow(()-> errors.exception("Could not find package [%s]. \n%s", path,
            ModuleMetadataPrinter.print(moduleLoader.getModuleMetadata(path))));
  }

  private boolean isAllImport(NamePath path) {
    return path.getLast().equals(ReservedName.ALL);
  }

  private boolean loadAllNamespaceObjects(SqrlModule module, Namespace ns) {
    return module.getNamespaceObjects().stream()
        .allMatch(obj -> ns.addNsObject(obj));
  }

  private Optional<NamespaceObject> getNamespaceObject(SqrlModule module, NamePath path) {
    return module.getNamespaceObject(path.getLast());
  }

  private Name getObjectName(Name last, Optional<SqlIdentifier> alias) {
    return alias.map(a-> Name.system(a.names.get(0))).orElse(last);
  }
}

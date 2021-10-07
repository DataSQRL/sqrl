package ai.dataeng.sqml.imports;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.QualifiedName;

public class ImportAnalyzer extends ImportVisitor<Scope, Scope> {
  private final Metadata metadata;
  private final ImportDefinition node;
  private final Analysis analysis;

  public ImportAnalyzer(Metadata metadata, ImportDefinition node, Analysis analysis) {
    this.metadata = metadata;
    this.node = node; //todo: remove node here?
    this.analysis = analysis;
  }

  @Override
  public Scope visitScript(ScriptImportObject object, Scope context) {
    return super.visitScript(object, context);
  }

  @Override
  public Scope visitTable(TableImportObject object, Scope context) {

    String name = object.getPath();
    int partsIdx = name.lastIndexOf(".");
    String path = name.substring(0, partsIdx);
    String tableName = name.substring(partsIdx + 1);
    object.getMapping();

//
//    SourceDataset dataset = metadata.getEnv().getDdRegistry().getDataset(path);
//    SourceTable sourceTable = dataset.getTable(tableName.toLowerCase(Locale.ROOT));
//    SourceTableStatistics stats = metadata.getEnv().getDdRegistry()
//        .getTableStatistics(sourceTable);

    return null;
  }

  @Override
  public Scope visitFunction(FunctionImportObject object, Scope context) {
    analysis.addFunction(object.getFunction());
    metadata.getFunctionProvider().add(object.getName(), object.getFunction());
    return super.visitFunction(object, context);
  }
}

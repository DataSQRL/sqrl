package ai.dataeng.sqml;

import ai.dataeng.sqml.StubModel.ModelRelation;
import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.connector.Source;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.relation.TableHandle;
import ai.dataeng.sqml.sql.tree.Script;
import java.util.List;
import java.util.Optional;

/**
 * Creates or migrates an existing schema
 */
public class SchemaManager {

  private final Metadata metadata;

  public SchemaManager(Metadata metadata) {
    this.metadata = metadata;
  }

  public void init(List<Source> sources, Script script, Analysis scriptAnalysis) {
    for (Source source : sources) {
      createOrMigrateSource(source);
    }

    createOrMigrateScript(script, scriptAnalysis);
  }

  private void createOrMigrateScript(Script script, Analysis scriptAnalysis) {
    for (ModelRelation rel : scriptAnalysis.createStubModel(metadata).relations) {
      metadata.createView(rel);
    }
  }

  private void createOrMigrateSource(Source source) {
    Optional<TableHandle> tableHandle = metadata.getSourceTable(source);
    if (tableHandle.isEmpty()) {
      metadata.createSourceTable(source);
    }
  }
}

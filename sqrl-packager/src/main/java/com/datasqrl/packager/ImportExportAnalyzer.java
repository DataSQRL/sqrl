package com.datasqrl.packager;

import com.datasqrl.loaders.Exporter;
import com.datasqrl.loaders.Loader;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.local.generate.Resolve;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Value;
import org.apache.calcite.sql.ExportDefinition;
import org.apache.calcite.sql.ImportDefinition;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.ListUtils;


public class ImportExportAnalyzer {

  NameCanonicalizer canonicalizer = NameCanonicalizer.SYSTEM;
  SqrlParser parser = new SqrlParser();

  public Result analyze(Path sqrlScript) {
    try {
      return analyze(parser.parse(Files.readString(sqrlScript)));
    } catch (Throwable e) {
      throw new IllegalArgumentException(String.format("Could not parse script: [%s]",sqrlScript),e);
    }
  }

  private Result analyze(ScriptNode scriptNode) {
    List<NamePath> importPkgs = new ArrayList(), exportPkgs = new ArrayList<>();
    for (SqlNode statement : scriptNode.getStatements()) {
      if (statement instanceof ImportDefinition) {
        ImportDefinition impDef = (ImportDefinition) statement;
        importPkgs.add(Resolve.toNamePath(canonicalizer, impDef.getImportPath()).popLast());
      }
      if (statement instanceof ExportDefinition) {
        ExportDefinition expDef = (ExportDefinition) statement;
        exportPkgs.add(Resolve.toNamePath(canonicalizer, expDef.getSinkPath()).popLast());
      }
    }
    return new Result(importPkgs, exportPkgs);
  }

  @Value
  public static class Result {

    public static final Result EMPTY = new Result(Collections.emptyList(), Collections.emptyList());

    List<NamePath> importPkgs;
    List<NamePath> exportPkgs;

    public Result add(Result other) {
      return new Result(ListUtils.union(importPkgs,other.importPkgs),
          ListUtils.union(exportPkgs, other.exportPkgs));
    }

    public List<NamePath> getUnloadableDependencies(Path basePath, Loader loader, Exporter exporter) {
      List<NamePath> unloadable = new ArrayList<>();
      importPkgs.stream().filter(np -> !loader.isPackage(basePath, np)).forEach(unloadable::add);
      exportPkgs.stream().filter(np -> !exporter.isPackage(basePath, np)).forEach(unloadable::add);
      return unloadable;
    }

  }


}

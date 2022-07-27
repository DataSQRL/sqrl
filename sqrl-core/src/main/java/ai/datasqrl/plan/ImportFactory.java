//package ai.datasqrl.plan;
//
//import ai.datasqrl.environment.ImportManager.SourceTableImport;
//import ai.datasqrl.environment.ImportManager.TableImport;
//import ai.datasqrl.parse.tree.ImportDefinition;
//import ai.datasqrl.parse.tree.name.Name;
//import ai.datasqrl.plan.local.ImportedTable;
//import java.util.List;
//
//public class ImportFactory {
//
//  public List<ImportSchema> resolve(ImportDefinition node) {
//    Name sourceDataset = node.getNamePath().get(0);
//    TableImport tblImport = importManager.importTable(sourceDataset, sourceTable,
//        schemaSettings, errors);
//
//
//    SourceTableImport sourceTableImport = (SourceTableImport)tblImport;
//
//    ImportedTable importedTable = tableFactory.importTable(sourceTableImport, node.getAliasName());
//    schemas.importTable(importedTable);
//
//    return null;
//  }
//}

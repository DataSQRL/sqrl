//package ai.dataeng.sqml.parser.sqrl;
//
//import ai.dataeng.sqml.parser.SqrlParser;
//import ai.dataeng.sqml.parser.operator.ImportResolver;
//import ai.dataeng.sqml.parser.sqrl.operations.SqrlOperation;
//import ai.dataeng.sqml.schema.Namespace;
//import ai.dataeng.sqml.schema.NamespaceImpl;
//import java.util.List;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//public class ImportTest {
//  Namespace namespace;
//  ImportProcessor importProcessor;
//  SqrlParser sqrlParser;
//
//  @BeforeEach
//  public void setup() {
//    sqrlParser = new SqrlParser();
//    namespace = new NamespaceImpl();
//    importProcessor = new ImportProcessor(namespace, new ImportResolver());
//  }
//
//  @Test
//  public void testImport() {
//    List<SqrlOperation> operation = process("IMPORT ecommerce-data.Orders");
//  }
//
//  @Test
//  public void testImportAlias() {
//    List<SqrlOperation> operation = process("IMPORT ecommerce-data.Orders AS _Orders");
//  }
//
//  @Test
//  public void testImportStar() {
//    List<SqrlOperation> operation = process("IMPORT ecommerce-data.*");
//  }
//
//  @Test
//  public void testImportInvalidStarRename() {
//    List<SqrlOperation> operation = process("IMPORT ecommerce-data.* AS invalid");
//  }
//
//  @Test
//  public void testImportInvalidNamespaceRename() {
//    List<SqrlOperation> operation = process("IMPORT ecommerce-data AS invalid");
//  }
//
//  @Test
//  public void testImportNamespace() {
//    List<SqrlOperation> operation = process("IMPORT ecommerce-data");
//  }
//
//  @Test
//  public void testImportFunction() {
//    List<SqrlOperation> operation = process("IMPORT time.roundToMonth");
//  }
//
//  @Test
//  public void testImportFunctionAlias() {
//    List<SqrlOperation> operation = process("IMPORT time.roundToMonth AS toMonth");
//  }
//
//  public List<SqrlOperation> process(String imp) {
//    return importProcessor.process(sqrlParser.parseStatement(imp));
//  }
//}

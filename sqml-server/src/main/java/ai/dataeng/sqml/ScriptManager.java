//package ai.dataeng.sqml;
//
//import ai.dataeng.sqml.analyzer.ScriptAnalyzer;
//import ai.dataeng.sqml.metadata.Metadata;
//import ai.dataeng.sqml.sql.parser.ParsingOptions;
//import ai.dataeng.sqml.sql.parser.SqlParser;
//import ai.dataeng.sqml.sql.tree.Script;
//import java.util.List;
//import java.util.Optional;
//
//public class ScriptManager {
//  SqlParser sqlParser = new SqlParser();
//  Session session = null;
//  Metadata metadata = null;
//  GraphqlServletManager graphqlServletManager = null;
//  IngressManager ingressManager = null;
//
//  public void loadScript(String name, String script,
//      Optional<String> ingestFile) {
//    Script parserScript = sqlParser.createScript(script, new ParsingOptions());
//
//    ScriptAnalyzer analyzer = new ScriptAnalyzer(session, sqlParser, List.of(), metadata);
//    ScriptAnalysis analysis = analyzer.analyze(parserScript);
//
//    graphqlServletManager.loadGraphqlServer(name, schemaFactory.build(analysis));
//
//    ingestFile.ifPresent((ingest)->ingressManager.load(ingest));
//
//    System.out.println("Load:" + name + ":" + script);
//  }
//
//  public void loadIngest(String name, String ingestScript) {
//
//  }
//
//  public boolean unloadScript(String name) {
//    System.out.println("Unload:" + name);
//
//    return true;
//  }
//}

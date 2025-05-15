/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// package com.datasqrl.util;
//
// import com.datasqrl.function.SqrlFunction;
// import com.datasqrl.functions.flink.FlinkStdLibraryImpl;
// import com.datasqrl.functions.json.StdJsonLibraryImpl;
// import com.datasqrl.functions.secure.StdSecureLibraryImpl;
// import com.datasqrl.functions.string.StdStringLibraryImpl;
// import com.datasqrl.functions.text.StdTextLibraryImpl;
// import com.datasqrl.functions.time.StdTimeLibraryImpl;
// import com.datasqrl.serializer.Deserializer;
// import com.datasqrl.module.SqrlModule;
// import com.datasqrl.function.CalciteFunctionNsObject;
// import com.datasqrl.module.FunctionNamespaceObject;
// import com.datasqrl.module.NamespaceObject;
// import com.datasqrl.functions.vector.StdVectorLibraryImpl;
// import com.datasqrl.time.TimeTumbleWindowFunction;
// import com.google.common.base.Preconditions;
// import com.theokanning.openai.completion.CompletionChoice;
// import com.theokanning.openai.completion.CompletionRequest;
// import com.theokanning.openai.service.OpenAiService;
// import java.io.IOException;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.function.Consumer;
// import lombok.SneakyThrows;
// import lombok.Value;
// import org.apache.calcite.sql.SqlFunction;
//
// public class GenerateFunctionDocumentation {
//
//  public static final Path SQL_FUNCTION_DOCS_FILE = Paths.get("docs", "automated", "sql",
// "sqlFunctionDocs.json");
//  public static final String START_FIRST_SENTENCE = "This SQL function returns";
//  public static final String START_SECOND_SENTENCE = "For example";
//
//  private OpenAiService service;
//
//  private int numAPICalls = 0;
//
//  private Map<String,String> sqlFunctionDocs;
//
//  private boolean useOpenAI = true;
//
//  public GenerateFunctionDocumentation(String apiKey) {
//    this.service = new OpenAiService(apiKey);
//    if (Files.isRegularFile(SQL_FUNCTION_DOCS_FILE)) {
//      sqlFunctionDocs = new Deserializer().mapJsonFile(SQL_FUNCTION_DOCS_FILE, Map.class);
//    } else {
//      sqlFunctionDocs = new HashMap<>();
//    }
//  }
//
//  public void saveSQLFunctionDocs() throws IOException {
//    Files.createDirectories(SQL_FUNCTION_DOCS_FILE.getParent());
//    new Deserializer().writeJson(SQL_FUNCTION_DOCS_FILE, sqlFunctionDocs, true);
//  }
//
//  @SneakyThrows
//  public static void main(String[] args) {
//    String openAPIKey = args.length>0?args[0]:"";
//    GenerateFunctionDocumentation docs = new GenerateFunctionDocumentation(openAPIKey);
//    System.out.println(docs.generateFunctionMarkdownDocumentation(libraries.get(6)));
//    docs.saveSQLFunctionDocs();
//    System.out.println("Number of API calls: "  + docs.numAPICalls);
//  }
//
//  private String lookUpSQLDocs(String functionName) {
//    functionName = functionName.toLowerCase();
//    if (sqlFunctionDocs.containsKey(functionName)) {
//      return sqlFunctionDocs.get(functionName);
//    } else if (useOpenAI) {
//      String docs = generateSQLDocs(functionName);
//      sqlFunctionDocs.put(functionName,docs);
//      return docs;
//    } else {
//      return "coming soon";
//    }
//  }
//
//  private String generateSQLDocs(String functionName) {
//    functionName = functionName.toUpperCase();
//    CompletionRequest request = CompletionRequest.builder()
//        .model("text-davinci-003")
//        .temperature(0.0)
//        .maxTokens(50)
//        .topP(1.0)
//        .frequencyPenalty(0.0)
//        .presencePenalty(0.0)
//        .prompt(
//            "Write one sentence of documentation explaining the SQL function \"" + functionName
//                + "\". Add one sentence starting with \"" + START_SECOND_SENTENCE
//                + "\" showing an example invocation and result of the function with specific data.
// "
//                + START_FIRST_SENTENCE)
//        .build();
//    List<CompletionChoice> completion = service.createCompletion(request).getChoices();
//    numAPICalls++;
//    Preconditions.checkArgument(completion.size() == 1);
//    return START_FIRST_SENTENCE + " " + completion.get(0).getText();
//  }
//
//  public String generateFunctionMarkdownDocumentation(LibrarySpec libSpec) {
//    StringBuilder s = new StringBuilder();
//    List<NamespaceObject> allFcts = new ArrayList<>(libSpec.library.getNamespaceObjects());
//    Collections.sort(allFcts, (ns1, ns2) -> ns1.getName().compareTo(ns2.getName()));
//    for (NamespaceObject fct : allFcts) {
//      int arrayLength = 2;
//      final int timeWindowOffset = libSpec.withTimeWindow?arrayLength++:-1;
//      final int sqlNameOffset = libSpec.withSqlName?arrayLength++:-1;
//      String[] columns = new String[arrayLength];
//      Consumer<Boolean> isTimeWindow = arg -> {
//        if (libSpec.withTimeWindow)
//          columns[timeWindowOffset] = (arg ? "yes" : "no");
//      };
//      Consumer<String> sqlNameSetter = arg -> {
//        if (libSpec.withSqlName) columns[sqlNameOffset] = arg;
//      };
//      if (fct instanceof FunctionNamespaceObject) {
//        FunctionNamespaceObject fObj = (FunctionNamespaceObject) fct;
//        String functionName = fct.getName().getDisplay();
//        columns[0] = functionName;
//        if (fObj.getFunction() instanceof SqrlFunction) {
//          SqrlFunction function = (SqrlFunction) fObj.getFunction();
//          columns[1] = function.getDocumentation();
//          isTimeWindow.accept(function instanceof TimeTumbleWindowFunction);
//          sqlNameSetter.accept("-");
//        } else if (fObj.getFunction() instanceof SqlFunction) {
//          String sqlName = ((CalciteFunctionNsObject)fct).getSqlName().toUpperCase();
//          String sqlDocs = lookUpSQLDocs(sqlName);
//          //replace SQL name with SQRL name
//          sqlDocs = sqlDocs.replace(sqlName+"(",functionName+"(");
//          columns[1] = sqlDocs;
//          isTimeWindow.accept(false);
//          sqlNameSetter.accept(sqlName);
//        } else {
//          throw new UnsupportedOperationException(
//              "Unexpected function type: " + fObj.getFunction().getClass());
//        }
//      }
//      addRow(s, columns);
//    }
//    return s.toString();
//  }
//
//  private static StringBuilder addRow(StringBuilder s, String... columns) {
//    Preconditions.checkArgument(columns.length > 0);
//    s.append("| ");
//    for (String col : columns) {
//      s.append(col).append(" | ");
//    }
//    s.append("\n");
//    return s;
//  }
//
//
//  @Value
//  public static class LibrarySpec {
//
//    SqrlModule library;
//    boolean withTimeWindow;
//    boolean withSqlName;
//
//  }
//
//  public static List<LibrarySpec> libraries = List.of(
//      new LibrarySpec(new StdStringLibraryImpl(), false, true),
//      new LibrarySpec(new StdTimeLibraryImpl(), true, false),
//      new LibrarySpec(new FlinkStdLibraryImpl(), true, false),
//      new LibrarySpec(new StdTextLibraryImpl(), false, false),
//      new LibrarySpec(new StdSecureLibraryImpl(), false, false),
//      new LibrarySpec(new StdJsonLibraryImpl(), false, false),
//      new LibrarySpec(new StdVectorLibraryImpl(), false, false)
//  );
//
// }

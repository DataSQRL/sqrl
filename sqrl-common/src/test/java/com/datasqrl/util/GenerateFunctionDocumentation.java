package com.datasqrl.util;

import com.datasqrl.function.SqrlFunction;
import com.datasqrl.function.builtin.string.StdStringLibraryImpl;
import com.datasqrl.function.builtin.time.StdTimeLibraryImpl.TimeWindowBucketFunction;
import com.datasqrl.loaders.SqrlModule;
import com.datasqrl.plan.local.generate.CalciteFunctionNsObject;
import com.datasqrl.plan.local.generate.FunctionNamespaceObject;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.google.common.base.Preconditions;
import com.theokanning.openai.completion.CompletionChoice;
import com.theokanning.openai.completion.CompletionRequest;
import com.theokanning.openai.service.OpenAiService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.calcite.sql.SqlFunction;

public class GenerateFunctionDocumentation {

  public static final String START_FIRST_SENTENCE = "This SQL function returns";
  public static final String START_SECOND_SENTENCE = "For example";

  private OpenAiService service;

  private boolean useOpenAI = true;

  public GenerateFunctionDocumentation(String apiKey) {
    this.service = new OpenAiService(apiKey);
  }

  public static void main(String[] args) {
    String openAPIKey = args.length>0?args[0]:"";
    GenerateFunctionDocumentation docs = new GenerateFunctionDocumentation(openAPIKey);
    SqrlModule fctModule = new StdStringLibraryImpl();
    System.out.println(docs.generateFunctionMarkdownDocumentation(fctModule, false));
  }

  private String generateDocs(String functionName) {
    functionName = functionName.toUpperCase();
    if (useOpenAI) {
      CompletionRequest request = CompletionRequest.builder()
          .model("text-davinci-003")
          .temperature(0.0)
          .maxTokens(50)
          .topP(1.0)
          .frequencyPenalty(0.0)
          .presencePenalty(0.0)
          .prompt(
              "Write one sentence of documentation explaining the SQL function \"" + functionName
                  + "\". Add one sentence starting with \"" + START_SECOND_SENTENCE
                  + "\" showing an example invocation and result of the function with specific data. "
                  + START_FIRST_SENTENCE)
          .build();
      List<CompletionChoice> completion = service.createCompletion(request).getChoices();
      Preconditions.checkArgument(completion.size() == 1);
      return START_FIRST_SENTENCE + " " + completion.get(0).getText();
    } else {
      return "The standard SQL function `" + functionName + "`";
    }
  }

  public String generateFunctionMarkdownDocumentation(SqrlModule fctModule,
      boolean withTimeWindow) {
    StringBuilder s = new StringBuilder();
    List<NamespaceObject> allFcts = new ArrayList<>(fctModule.getNamespaceObjects());
    Collections.sort(allFcts, (ns1, ns2) -> ns1.getName().compareTo(ns2.getName()));
    for (NamespaceObject fct : allFcts) {
      String[] columns = new String[2 + (withTimeWindow ? 1 : 0)];
      Consumer<Boolean> isTimeWindow = arg -> {
        if (withTimeWindow)
          columns[2] = (arg ? "yes" : "no");
      };
      if (fct instanceof FunctionNamespaceObject) {
        FunctionNamespaceObject fObj = (FunctionNamespaceObject) fct;
        columns[0] = fct.getName().getDisplay();
        if (fObj.getFunction() instanceof SqrlFunction) {
          SqrlFunction function = (SqrlFunction) fObj.getFunction();
          columns[1] = function.getDocumentation();
          isTimeWindow.accept(function instanceof TimeWindowBucketFunction);
        } else if (fObj.getFunction() instanceof SqlFunction) {
          SqlFunction function = (SqlFunction) fObj.getFunction();
          columns[1] = generateDocs(((CalciteFunctionNsObject)fct).getSqlName().toLowerCase());
          isTimeWindow.accept(false);
        } else {
          throw new UnsupportedOperationException(
              "Unexpected function type: " + fObj.getFunction().getClass());
        }
      }
      addRow(s, columns);
    }
    return s.toString();
  }

  private static StringBuilder addRow(StringBuilder s, String... columns) {
    Preconditions.checkArgument(columns.length > 0);
    s.append("|");
    for (String col : columns) {
      s.append(col).append("|");
    }
    s.append("\n");
    return s;
  }


}

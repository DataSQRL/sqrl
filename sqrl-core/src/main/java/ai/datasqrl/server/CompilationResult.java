package ai.datasqrl.server;

import ai.datasqrl.config.error.ErrorMessage;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class CompilationResult {

  private final int compileTime;
  private final Status status;
  private final List<Compilation> compilations;
  private final List<ErrorMessage> messages;

  public enum Status {
    success, failed
  }

  @Value
  @Builder
  public static class Compilation {

    private final String name;
    private final String filename;
    private final String preschema;
  }

  public static CompilationResult generateDefault(ScriptBundle bundle, long compileTimeMs) {
    assert compileTimeMs < Integer.MAX_VALUE;
    List<Compilation> compilations = new ArrayList<>();
    for (SqrlScript script : bundle.getScripts().values()) {
      compilations.add(Compilation.builder()
          .name(script.getName().getDisplay())
          .filename(script.getFilename())
          .preschema("")
          .build());
    }
    return CompilationResult.builder()
        .status(Status.success)
        .compileTime((int) compileTimeMs)
        .messages(Collections.EMPTY_LIST)
        .compilations(compilations).build();

  }

}

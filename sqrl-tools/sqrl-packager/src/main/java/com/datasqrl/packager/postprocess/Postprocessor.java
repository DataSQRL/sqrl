package com.datasqrl.packager.postprocess;

import com.datasqrl.compile.Compiler.CompilerResult;
import java.nio.file.Path;
import java.util.Optional;
import lombok.Value;

public interface Postprocessor {

  public void process(ProcessorContext context);

  @Value
  public class ProcessorContext {
    Path buildDir;
    Path targetDir;
    CompilerResult compilerResult;
    Optional<Path> mountDir;
    String[] profiles;
  }
}

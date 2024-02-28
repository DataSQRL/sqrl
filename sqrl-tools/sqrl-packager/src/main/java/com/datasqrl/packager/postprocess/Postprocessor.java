package com.datasqrl.packager.postprocess;

import java.nio.file.Path;
import java.util.Optional;
import lombok.Value;

public interface Postprocessor {

  public void process(ProcessorContext context);

  @Value
  public class ProcessorContext {
    Path buildDir;
    Path targetDir;
    Optional<Path> mountDir;
    String[] profiles;
  }
}

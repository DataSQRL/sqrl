package com.datasqrl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

public class SqrlCompiler {


  public void execute(Path path, String... args) {
    String jarPath = Path.of("../../sqrl-tools/sqrl-cli/target/sqrl-cli.jar").toAbsolutePath().toString(); // Set the path to your JAR file

    // Convert the args array to a single command string for the process
    String[] command = new String[args.length + 3];
    command[0] = "java";
    command[1] = "-jar";
    command[2] = jarPath;
    System.arraycopy(args, 0, command, 3, args.length);

    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.directory(path.toFile()); // Set the working directory to the provided path
    processBuilder.redirectErrorStream(true); // Redirect error stream to standard output

    try {
      Process process = processBuilder.start();

      // Print the output from the process
      process.getInputStream().transferTo(System.out);

      // Wait for the process to complete and get the exit value
      int exitCode = process.waitFor();
      System.out.println("Process exited with code: " + exitCode);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}

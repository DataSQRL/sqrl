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
// package com.datasqrl.flink;
//
// import java.io.IOException;
// import java.lang.reflect.Constructor;
// import java.lang.reflect.Method;
// import java.net.URL;
// import java.net.URLClassLoader;
// import java.nio.file.Files;
// import java.nio.file.Path;
//
// public class SqrlCompiler {
//
//
//  public void execute(Path path, String... args) {
//    String jarPath =
// Path.of("../../sqrl-tools/sqrl-cli/target/sqrl-cli.jar").toAbsolutePath().toString(); // Set the
// path to your JAR file
//
//    // Convert the args array to a single command string for the process
//    String[] command = new String[args.length + 3];
//    command[0] = "java";
//    command[1] = "-jar";
//    command[2] = jarPath;
//    System.arraycopy(args, 0, command, 3, args.length);
//
//    ProcessBuilder processBuilder = new ProcessBuilder(command);
//    processBuilder.directory(path.toFile()); // Set the working directory to the provided path
//    processBuilder.redirectErrorStream(true); // Redirect error stream to standard output
//
//    try {
//      Process process = processBuilder.start();
//
//      // Print the output from the process
//      process.getInputStream().transferTo(System.out);
//
//      // Wait for the process to complete and get the exit value
//      int exitCode = process.waitFor();
//      System.out.println("Process exited with code: " + exitCode);
//    } catch (IOException | InterruptedException e) {
//      e.printStackTrace();
//    }
//  }
// }

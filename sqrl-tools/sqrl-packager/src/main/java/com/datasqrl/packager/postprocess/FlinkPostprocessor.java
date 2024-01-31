package com.datasqrl.packager.postprocess;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkPostprocessor implements Postprocessor {

  public static String getInitFlink() {
    return "#!/bin/bash\n"
        + "\n"
        + "# Copy the JAR file to the plugins directory\n"
        + "mkdir -p /opt/flink/plugins/s3-fs-presto\n"
        + "cp /opt/flink/opt/flink-s3-fs-presto-1.16.1.jar /opt/flink/plugins/s3-fs-presto/\n"
        + "# Execute the passed command\n"
        + "exec /docker-entrypoint.sh \"$@\"\n";
  }

  public static String getFlinkExecute() {
    return "#!/bin/sh\n"
        + "\n"
        + "while ! curl -s http://flink-jobmanager:8081/overview | grep -q '\"taskmanagers\":1'; do\n"
        + "  echo 'Waiting for Flink JobManager REST API...';\n"
        + "  sleep 5;\n"
        + "done;\n"
        + "echo 'Submitting Flink job...';\n"
        + "upload_response=$(curl -X POST -H \"Expect:\" -F \"jarfile=@flink-job.jar\" http://flink-jobmanager:8081/jars/upload);\n"
        + "echo \"$upload_response\";\n"
        + "jar_id=$(echo \"$upload_response\" | jq -r '.filename');\n"
        + "echo \"$jar_id\";\n"
        + "filename=$(echo \"$jar_id\" | awk -F'/' '{print $NF}');\n"
        + "sleep 3;\n"
        + "echo \"$filename\"\n"
        + "echo \"curl -X POST \"http://flink-jobmanager:8081/jars/${filename}/run\";\";\n"
        + "post_response=$(curl -X POST \"http://flink-jobmanager:8081/jars/${filename}/run\");\n"
        + "echo \"$post_response\";\n"
        + "echo 'Job submitted.'";
  }

  @Override
  public void process(ProcessorContext context) {
    addFlinkExecute(context.getTargetDir());
    addInitFlink(context.getTargetDir());
  }

  protected void addFlinkExecute(Path targetDir) {
    String content = getFlinkExecute();
    copyExecutableFile("submit-flink-job.sh", content, targetDir);
  }

  protected void addInitFlink(Path targetDir) {
    String content = getInitFlink();
    copyExecutableFile("init-flink.sh", content, targetDir);
  }

  protected void copyExecutableFile(String fileName, String content, Path targetDir) {
    Path toFile = targetDir.resolve(fileName);
    try {
      Files.createDirectories(targetDir);
      Files.writeString(toFile, content);

      Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxr-xr-x");
      Files.setPosixFilePermissions(toFile, perms);

    } catch (Exception e) {
      log.error("Could not copy flink executor file.");
      throw new RuntimeException(e);
    }
  }
}

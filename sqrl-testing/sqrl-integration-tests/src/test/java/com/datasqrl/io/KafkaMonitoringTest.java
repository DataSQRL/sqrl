package com.datasqrl.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.cmd.RootCommand;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class KafkaMonitoringTest extends KafkaBaseTest {

  protected SnapshotTest.Snapshot snapshot;

  protected Path rootDir = Path.of("");
  protected Path writeToDir = rootDir.resolve("output");

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    FileUtil.deleteDirectory(writeToDir);
    if (!Files.isDirectory(writeToDir)) {
      Files.createDirectory(writeToDir);
    }
  }

  @AfterEach
  @SneakyThrows
  public void tearDown() {
    FileTestUtil.readAllFilesInDirectory(writeToDir, ".yml").forEach((name, content) -> snapshot.addContent(content, name));
    FileUtil.deleteDirectory(writeToDir);
    snapshot.createOrValidate();
  }

  @Test
  @SneakyThrows
  public void monitorOrdersTest() {
    String topic = "orders";
    createTopics(new String[]{topic});
    initialize(IntegrationTestSettings.getFlinkWithDB());

    int recordsWritten = writeTextFilesToTopic(topic, Retail.INSTANCE.getDataDirectory().resolve("orders.json"));
    assertEquals(4, recordsWritten);

    TableConfig systemConfig = getSystemConfigBuilder("retail",false);
    monitor(systemConfig);
  }

  @Test
  @SneakyThrows
  public void monitor2OrdersTest() {
    String[] topics = {"example.orders1", "example.orders2"};
    createTopics(topics);
    initialize(IntegrationTestSettings.getFlinkWithDB());

    int recordsWritten = writeTextFilesToTopic(topics[0], Retail.INSTANCE.getDataDirectory().resolve("orders.json"));
    assertEquals(4, recordsWritten);
    recordsWritten = writeTextFilesToTopic(topics[1], Nutshop.INSTANCE.getDataDirectory().resolve("orders_part1.json"));
    assertEquals(87, recordsWritten);

    TableConfig systemConfig = getSystemConfigBuilder("example", true);
    monitor(systemConfig);
  }

  @SneakyThrows
  private void monitor(TableConfig config) {
    Path configFile = Files.createTempFile(rootDir, "system.table", ".json");
    config.toFile(configFile);
    try {
      new RootCommand(rootDir).getCmd().execute("discover",
          configFile.toString(), "-o", writeToDir.toString(), "-l", "7");
    } finally {
      Files.deleteIfExists(configFile);
    }
  }


}

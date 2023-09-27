//package com.datasqrl;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.runtime.client.JobStatusMessage;
//import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
//import org.apache.flink.test.util.MiniClusterWithClientResource;
//import org.junit.jupiter.api.extension.AfterEachCallback;
//import org.junit.jupiter.api.extension.BeforeAllCallback;
//import org.junit.jupiter.api.extension.BeforeEachCallback;
//import org.junit.jupiter.api.extension.ExtensionContext;
//
//@Slf4j
//public class FlinkClusterExtension implements AfterEachCallback, BeforeEachCallback,
//    BeforeAllCallback, AfterEachCallback {
//
//  private static final int DEFAULT_PARALLELISM = 4;
//
//  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
//      new MiniClusterWithClientResource(
//          new MiniClusterResourceConfiguration.Builder()
//              .setNumberTaskManagers(1)
//              .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
//              .build());
//
//  @Override
//  public void afterEach(ExtensionContext context) throws Exception {
//    MINI_CLUSTER_RESOURCE.after();
//    if (!MINI_CLUSTER_RESOURCE.getMiniCluster().isRunning()) {
//      // do nothing if the MiniCluster is not running
//      log.warn("Mini cluster is not running after the test!");
//      return;
//    }
//
//    for (JobStatusMessage path : MINI_CLUSTER_RESOURCE.getClusterClient().listJobs().get()) {
//      if (!path.getJobState().isTerminalState()) {
//        try {
//          MINI_CLUSTER_RESOURCE.getClusterClient().cancel(path.getJobId()).get();
//        } catch (Exception ignored) {
//          // ignore exceptions when cancelling dangling jobs
//        }
//      }
//    }
//  }
//
//  @Override
//  public void beforeEach(ExtensionContext context) throws Exception {
//    MINI_CLUSTER_RESOURCE.before();
//  }
//
//  @Override
//  public void beforeAll(ExtensionContext context) throws Exception {
//    MINI_CLUSTER_RESOURCE.before();
//  }
//}

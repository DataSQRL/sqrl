version: "3.8"
services:
  test:
    build: test
    volumes:
      - ./test:/test
<#if config["compiler"]["snapshotPath"]??>
      - ${config["compiler"]["snapshotPath"]}:/test/snapshots
</#if>
    command: ["jmeter", "-n", "-t", "/test/test-plan.jmx", "-l", "/test/results.jtl"]
    depends_on:
      server:
        condition: service_started
      flink-job-submitter:
        condition: service_completed_successfully

package com.datasqrl.util.data;

import com.datasqrl.util.TestScript;
import com.google.common.collect.ImmutableList;
import java.nio.file.Path;
import java.util.List;

public class Examples {

  static final Path base = Path.of("..", "sqrl-examples");
  public static List<TestScript> scriptList = ImmutableList.<TestScript>builder()
      .add(TestScript.of(base.resolve("art/poetry-cloud"),
              base.resolve("art/poetry-cloud/poetry.sqrl"))
          .resultTables(List.of("star", "post", "user", "connect", "newconnections"))
          .dataDir(base.resolve("art/poetry-data"))
          .build())
      .add(TestScript.of(base.resolve("crypto/bitcoin-tracer"),
              base.resolve("crypto/bitcoin-tracer/tracer.sqrl"))
          .resultTables(List.of("transactions"))
          .dataDir(base.resolve("crypto/bitcoin-data"))
          .build())
      .add(TestScript.of(base.resolve("cybersecurity/intrusion"),
          base.resolve("cybersecurity/intrusion/intrusion.sqrl"))
          .dataDir(base.resolve("cybersecurity/network-data"))
          .resultTables(List.of("traffic", "system-membership", "system", "minutetraffic", "hourtraffic",
              "instance", "outlieralert"))
          .build())
      .add(TestScript.of(base.resolve("datasqrl/repo"),
              base.resolve("datasqrl/repo/repo.sqrl"))
          .dataDir(base.resolve("datasqrl/datasqrl-central"))
          .resultTable("submission")
          .resultTable("package").build())
      .add(TestScript.of(base.resolve("ecommerce/public-api"),
          base.resolve("ecommerce/public-api/api.sqrl"))
          .dataDir(base.resolve("ecommerce/auction-data"))
          .resultTables(List.of("auction", "user", "bid", "category"))
          .build())
      .add(TestScript.of(base.resolve("ecommerce/internal-analytics"),
          base.resolve("ecommerce/internal-analytics/analytics.sqrl"))
          .dataDir(base.resolve("ecommerce/auction-data"))
          .resultTables(List.of("auction", "user", "bid", "category", "seller", "auctionsbyday", "highpriceauctions", "newusermonitoring"))
          .build())
      .add(TestScript.of(base.resolve("environment/monitoring"),
          base.resolve("environment/monitoring/monitoring.sqrl"))
          .dataDir(base.resolve("environment/sensor-data"))
          .resultTables(List.of("sensor", "sensor-reading", "wildfirealert", "brokensensoralert"))
          .build())
      .add(TestScript.of(base.resolve("financial/fraud-detection"),
          base.resolve("financial/fraud-detection/fraud.sqrl"))
          .dataDir(base.resolve("financial/cc-data"))
          .resultTables(List.of("merchant", "pos", "transactions", "creditcard", "usstates", "fraudincreasealert"))
          .build())
      .add(TestScript.of(base.resolve("gaming/weworkout"),
          base.resolve("gaming/weworkout/weworkout.sqrl"))
          .dataDir(base.resolve("gaming/exercise-data"))
          .resultTables(List.of("challenges", "membership", "user", "team", "region"))
          .build())
      .add(TestScript.of(base.resolve("iot/homegenie"),
          base.resolve("iot/homegenie/homegenie.sqrl"))
          .dataDir(base.resolve("iot/home-data"))
          .resultTables(List.of("home", "rule", "sensor", "sensor-metrics", "ruletriggeredevent"))
          .build())
      .add(TestScript.of(base.resolve("location/visitor-guide"),
          base.resolve("location/visitor-guide/guide.sqrl"))
          .dataDir(base.resolve("location/funtown-data"))
          .resultTables(List.of("locations", "tracking", "visitor", "capacityalert"))
          .build())
      .add(TestScript.of(base.resolve("logistics/operations"),
          base.resolve("logistics/operations/operations.sqrl"))
          .dataDir(base.resolve("logistics/tracking-data"))
          .resultTables(List.of("shipments", "transport-event", "warehouseinventory", "warehouse", "highinventory", "delayedshipments", "delayedshipmentsanalysis"))
          .build())
      .add(TestScript.of(base.resolve("logistics/tracking"),
          base.resolve("logistics/tracking/tracking.sqrl"))
          .dataDir(base.resolve("logistics/tracking-data"))
          .resultTables(List.of("shipments", "transport-event"))
          .build())
      .add(TestScript.of(base.resolve("media/cookout"),
              base.resolve("media/cookout/cookout.sqrl"))
          .dataDir(base.resolve("media/cookout-data"))
          .resultTables(List.of("review", "stream", "stream-event", "user", "view-event", "userrecommendationislive"))
          .build())
      .add(TestScript.of(base.resolve("medical/adr"),
              base.resolve("medical/adr/adr-detection.sqrl"))
          .dataDir(base.resolve("medical/adr-data"))
          .resultTables(List.of("adrtemplate", "drugingredients", "medical_history", "patient", "prescription", "adr_alert"))
          .build())
      .add(TestScript.of(base.resolve("military/platoon"),
              base.resolve("military/platoon/platoon.sqrl"))
          .dataDir(base.resolve("military/platform-data"))
          .resultTables(List.of("platform", "platform-tracking", "platformstatus", "platoon", "trackedforeignplatform"))
          .build())
      .add(TestScript.of(base.resolve("news/newsrank"),
              base.resolve("news/newsrank/newsrank.sqrl"))
          .dataDir(base.resolve("news/data"))
          .resultTables(List.of("comment", "post", "user", "vote", "newsfeed"))
          .build())
      .add(TestScript.of(base.resolve("social-network/recommendation"),
              base.resolve("social-network/recommendation/recommendation.sqrl"))
          .dataDir(base.resolve("social-network/network-data"))
          .resultTables(List.of("friendships", "groupmemberships", "groups", "postings", "users"))
          .build())
      .add(TestScript.of(base.resolve("social-network/fraud"),
              base.resolve("social-network/fraud/fraud.sqrl"))
          .dataDir(base.resolve("social-network/network-data"))
          .resultTables(List.of("friendships", "groupmemberships", "groups", "postings", "users", "fraudulentuseralert"))
          .build())
      .add(TestScript.of(base.resolve("social-network/social-features"),
              base.resolve("social-network/social-features/social-features.sqrl"))
          .dataDir(base.resolve("social-network/network-data"))
          .resultTables(List.of("friendships", "groupmemberships", "groups", "postings", "users"))
          .build())
      .add(TestScript.of(base.resolve("social-network/social-commons"),
              base.resolve("social-network/social-commons/social-commons.sqrl"))
          .dataDir(base.resolve("social-network/network-data"))
          .resultTables(List.of("friendships", "groupmemberships", "groups", "postings", "users"))
          .build())
      .add(TestScript.of(base.resolve("social-network/newsfeed"),
              base.resolve("social-network/newsfeed/newsfeed.sqrl"))
          .dataDir(base.resolve("social-network/network-data"))
          .resultTables(List.of("friendships", "groupmemberships", "groups", "postings", "users"))
          .build())
      .add(TestScript.of(base.resolve("system-monitoring/monitoring"),
              base.resolve("system-monitoring/monitoring/monitoring.sqrl"))
          .dataDir(base.resolve("system-monitoring/monitoring-data"))
          .resultTables(List.of("config", "metrics", "cluster", "nodecapacityalert", "nodeperformancealert", "clusterimbalancealert"))
          .build())
      .add(TestScript.of(base.resolve("telecommunications/user-portal"),
              base.resolve("telecommunications/user-portal/portal.sqrl"))
          .dataDir(base.resolve("telecommunications/telco-data"))
          .resultTables(List.of("requests", "sim"))
          .build())
      .add(TestScript.of(base.resolve("telecommunications/content-delivery"),
              base.resolve("telecommunications/content-delivery/content.sqrl"))
          .dataDir(base.resolve("telecommunications/telco-data"))
          .resultTables(List.of("requests", "tower", "content"))
          .build())
      .add(TestScript.of(base.resolve("transportation/operations"),
              base.resolve("transportation/operations/operations.sqrl"))
          .dataDir(base.resolve("transportation/bus-data"))
          .resultTables(List.of("bus", "busstop", "gps_tracking", "route", "routestops", "schedule", "schedulefollows", "todaysoperation", "operationsbyday"))
          .build())
      .add(TestScript.of(base.resolve("transportation/busbutler"),
              base.resolve("transportation/busbutler/busbutler.sqrl"))
          .dataDir(base.resolve("transportation/bus-data"))
          .resultTables(List.of("bus", "busstop", "gps_tracking", "route", "routestops", "schedule", "schedulefollows"))
          .build())
      .build();
}

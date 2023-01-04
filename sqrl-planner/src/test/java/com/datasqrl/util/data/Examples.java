package com.datasqrl.util.data;

import com.datasqrl.util.TestScript;
import com.google.common.collect.ImmutableList;
import java.nio.file.Path;
import java.util.List;

public class Examples {

  static final Path base = Path.of("..", "sqrl-examples");
  public static List<TestScript> scriptList = ImmutableList.<TestScript>builder()
      .add(TestScript.of(base.resolve("crypto/bitcoin-tracer"),
          base.resolve("crypto/bitcoin-tracer/tracer.sqrl")).build())
      .add(TestScript.of(base.resolve("gaming/weworkout"),
          base.resolve("gaming/weworkout/weworkout.sqrl")).build())
      .add(TestScript.of(base.resolve("transportation/operations"),
          base.resolve("transportation/operations/operations.sqrl")).build())
      .add(TestScript.of(base.resolve("transportation/busbutler"),
          base.resolve("transportation/busbutler/busbutler.sqrl")).build())
      .add(TestScript.of(base.resolve("cybersecurity/intrusion"),
          base.resolve("cybersecurity/intrusion/intrusion.sqrl")).build())
      .add(TestScript.of(base.resolve("cybersecurity/intrusion/build"),
          base.resolve("cybersecurity/intrusion/build/intrusion.sqrl")).build())
      .add(TestScript.of(base.resolve("ecommerce/public-api"),
          base.resolve("ecommerce/public-api/api.sqrl")).build())
      .add(TestScript.of(base.resolve("ecommerce/internal-analytics"),
          base.resolve("ecommerce/internal-analytics/analytics.sqrl")).build())
      .add(TestScript.of(base.resolve("financial/fraud-detection"),
          base.resolve("financial/fraud-detection/fraud.sqrl")).build())
      .add(TestScript.of(base.resolve("art/poetry-cloud"),
          base.resolve("art/poetry-cloud/poetry.sqrl")).build())
      .add(TestScript.of(base.resolve("location/visitor-guide"),
          base.resolve("location/visitor-guide/guide.sqrl")).build())
      .add(TestScript.of(base.resolve("retail"),
          base.resolve("retail/c360-orderstats.sqrl")).build())
      .add(TestScript.of(base.resolve("retail/c360"),
          base.resolve("retail/c360/c360.sqrl")).build())
      .add(TestScript.of(base.resolve("retail"),
          base.resolve("retail/c360-recommend.sqrl")).build())
      .add(TestScript.of(base.resolve("retail"),
          base.resolve("retail/c360-full.sqrl")).build())
      .add(TestScript.of(base.resolve("social-network/recommendation"),
          base.resolve("social-network/recommendation/recommendation.sqrl")).build())
      .add(TestScript.of(base.resolve("social-network/fraud"),
          base.resolve("social-network/fraud/fraud.sqrl")).build())
      .add(TestScript.of(base.resolve("social-network/social-features"),
          base.resolve("social-network/social-features/social-features.sqrl")).build())
      .add(TestScript.of(base.resolve("social-network/social-commons"),
          base.resolve("social-network/social-commons/social-commons.sqrl")).build())
      .add(TestScript.of(base.resolve("social-network/newsfeed"),
          base.resolve("social-network/newsfeed/newsfeed.sqrl")).build())
      .add(TestScript.of(base.resolve("military/platoon"),
          base.resolve("military/platoon/platoon.sqrl")).build())
      .add(TestScript.of(base.resolve("iot/homegenie"),
          base.resolve("iot/homegenie/homegenie.sqrl")).build())
      .add(TestScript.of(base.resolve("news/newsrank"),
          base.resolve("news/newsrank/newsrank.sqrl")).build())
      .add(TestScript.of(base.resolve("medical/adr"),
          base.resolve("medical/adr/adr-detection.sqrl")).build())
      .add(TestScript.of(base.resolve("environment/monitoring"),
          base.resolve("environment/monitoring/monitoring.sqrl")).build())
//      .add(TestScript.of(base.resolve("saas/productsavvy"),
//          base.resolve("saas/productsavvy/savvy.sqrl")).build())
      .add(TestScript.of(base.resolve("logistics/operations"),
          base.resolve("logistics/operations/operations.sqrl")).build())
      .add(TestScript.of(base.resolve("logistics/tracking"),
          base.resolve("logistics/tracking/tracking.sqrl")).build())
      .add(TestScript.of(base.resolve("telecommunications/user-portal"),
          base.resolve("telecommunications/user-portal/portal.sqrl")).build())
      .add(TestScript.of(base.resolve("telecommunications/content-delivery"),
          base.resolve("telecommunications/content-delivery/content.sqrl")).build())
      .add(TestScript.of(base.resolve("datasqrl"),
          base.resolve("datasqrl/repo.sqrl")).build())
      .add(TestScript.of(base.resolve("system-monitoring/monitoring"),
          base.resolve("system-monitoring/monitoring/monitoring.sqrl")).build())
      .add(TestScript.of(base.resolve("media/cookout"),
              base.resolve("media/cookout/cookout.sqrl"))
          .build())

      .build();
}

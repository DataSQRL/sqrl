package com.datasqrl;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class UseCasesFullIT extends UseCasesIT {

  @Test
  public void testBanking() {
    execute("banking", "loan.sqrl", "loan.graphqls");
  }

  @Test
  public void testClickstream() {
    execute("clickstream", "clickstream-teaser.sqrl", "clickstream-teaser.graphqls");
  }

  @Test
  public void testConference() {
    execute("conference", "conference.sqrl", "conference.graphqls");
  }

  @Test
  public void testSensorsMutation() {
    execute("test", "sensors", "sensors-mutation.sqrl", "sensors-mutation.graphqls", "sensors-mutation");
  }

  @Test
  public void testSensorsFull() {
    execute("test", "sensors", "sensors-full.sqrl", null, "sensors-full");
  }

  @Test
  public void testSeedshopExtended() {
    execute("test", "seedshop-tutorial", "seedshop-extended.sqrl", null, "seedshop-extended");
  }

  @Test
  @Disabled
  public void testSnowflake() {
    execute("snowflake", "snowflake.sqrl", null);
  }

  @Test
  public void testDuckdb() {
    compile("duckdb", "duckdb.sqrl", null);
  }

//
//  @Test
//  @Disabled
//  public void compile() {
//    compile("sensors", "sensors-mutation.sqrl", "sensors-mutation.graphqls");
//  }
//
//  @Test
//  @Disabled
//  public void testCompileScript() {
//    execute(Path.of("/Users/matthias/git/data-product-data-connect-cv/src/main/datasqrl"), AssertStatusHook.INSTANCE,
//        "compile", "clinical_views.sqrl", "-c", "test_package_clinical_views.json", "--profile", "profile/");
//  }
}

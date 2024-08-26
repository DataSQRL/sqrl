package com.datasqrl;

import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.Delete;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Statement;
import java.util.List;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * This test assumes a catalog integration is set up between aws glue and snowflake
 */
public class SnowflakeGlueIT {

  private S3Client s3Client;
  private String AWS_REGION = "us-west-2";
  private String AWS_SNOWFLAKE_ROLE = "iceberg_table_external_id_daniel";
  private String BUCKET_NAME = "my-bucket";
  private String ICEBERG_DATABASE_NAME = "mydb";
  private String catalogIntegrationName = "glueCatalogInt";
  private String AWS_GLUE_ROLE = "myGlueRole";
  private String icebergDatabaseName = "mydb";
  private String AWS_EXTERNAL_ID = "myid";
  private String awsAccountId = System.getenv("AWS_ACCOUNT_ID");
  private String snowflakeAccountId = System.getenv("SNOWFLAKE_ACCOUNT_ID");

  private Map<String, String> variableDefaults = Map.of(
      "ICEBERG_DATABASE_NAME", ICEBERG_DATABASE_NAME,
      "AWS_REGION", AWS_REGION,
      "AWS_GLUE_ROLE", AWS_GLUE_ROLE
      );

//  private Map<String, String> glueTemplate = Map.of("accountid", snowflakeAccountId,
//      "database-name", icebergDatabaseName);

  @BeforeEach
  public void setUp() {
    s3Client = S3Client.builder().region(Region.of(AWS_REGION)).build();
  }

  @AfterEach
  public void tearDown() {
    emptyS3Bucket(BUCKET_NAME);
  }

  /**
   * Must be executed by an aws user that has permission to create roles
   */
  @Disabled
  @Test
  public void createAwsSnowflakeIntegration() {
    //1. Create AWS resources
    createS3Bucket(BUCKET_NAME);
    createGlueCatalog(ICEBERG_DATABASE_NAME);

    //2. Add the snowflake-glue-iam.json policy
    String glueIam = applyTemplate(readFile("snowflake-glue-iam.json"));
    createAwsIAMPolicy(AWS_GLUE_ROLE, glueIam);

    //3. Create the catalog integration in snowflake
    executeSnowflake(applyTemplate(
        "CREATE CATALOG INTEGRATION ${SNOWFLAKE_INTEGRATION_NAME}\n"
        + "  CATALOG_SOURCE=GLUE\n"
        + "  CATALOG_NAMESPACE='${ICEBERG_DATABASE_NAME}'\n"
        + "  TABLE_FORMAT=ICEBERG\n"
        + "  GLUE_AWS_ROLE_ARN='arn:aws:iam::${AWS_ACCOUNT_ID}:role/${AWS_GLUE_ROLE}'\n"
        + "  GLUE_CATALOG_ID='${GLUE_CATALOG_ID}'\n"
        + "  GLUE_REGION='${AWS_REGION}'\n"
        + "  ENABLED=TRUE;"));

    createAWSRole(AWS_SNOWFLAKE_ROLE);
    attachIAMPolicy(AWS_GLUE_ROLE, AWS_SNOWFLAKE_ROLE);

    String s = "CREATE OR REPLACE EXTERNAL VOLUME ${SNOWFLAKE_VOLUME_NAME}\n"
        + "   STORAGE_LOCATIONS =\n"
        + "   (\n"
        + "    (\n"
        + "     NAME = 'iceberg_storage_role'\n"
        + "     STORAGE_PROVIDER = 'S3'\n"
        + "     STORAGE_BASE_URL = 's3://${S3_BUCKET_NAME}'\n"
        + "     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::${AWS_ACCOUNT_ID}:role/${AWS_SNOWFLAKE_ROLE}'\n"
        + "     STORAGE_AWS_EXTERNAL_ID = '${AWS_EXTERNAL_ID}'\n"
        + "    )\n"
        + "   );";


    //4. Get integration details
    Map<String, Object> result = executeSnowflake(applyTemplate(
        "DESCRIBE CATALOG INTEGRATION ${SNOWFLAKE_INTEGRATION_NAME};"));
    String GLUE_AWS_IAM_USER_ARN = (String) result.get("GLUE_AWS_IAM_USER_ARN");
    String GLUE_AWS_EXTERNAL_ID = (String) result.get("GLUE_AWS_EXTERNAL_ID");

    String d = applyTemplate(readFile("glue-assume-role-policy.json"),
        Map.of(
            "GLUE_AWS_IAM_USER_ARN", GLUE_AWS_IAM_USER_ARN,
            "GLUE_AWS_EXTERNAL_ID", GLUE_AWS_EXTERNAL_ID
        ));
    createAwsIAMPolicy("name", d);
    //5. Create assume-role-iam.json
  }


  @Disabled
  @Test
  public void destroyAwsSnowflakeIntegration() {
//    deleteS3Bucket(BUCKET_NAME);
//    deleteAWSRole();
//    deleteAWSRole();
//    deleteAWSPolicy();
//    deleteAWSPolicy();
//    deleteAWSGlue();
//
//    dropSnowflakeViews();
//    dropSnowflakeCatalog();
//    dropSnowflakeVolume();
  }


  private void attachIAMPolicy(String awsGlueRole, String awsSnowflakeRole) {

  }

  private void createAWSRole(String awsSnowflakeRole) {

  }

  //Creates an aws glue catalog
  private void createGlueCatalog(String ICEBERG_DATABASE_NAME) {


  }

  private void createAwsIAMPolicy(String name, String policyJson) {

  }

  private String readFile(String s) {

    return null;
  }


  private String applyTemplate(String s, Map<String, String> variables) {
    return null;
  }

  /**
   * Replaces ${var} with the template
   */
  private String applyTemplate(String string) {
    //Reads from addlVariables, System.getEnv() and variableDefaults to interpolate variables.
    //If non can be found, it should error
    return null;
  }

  private void createS3Bucket(String BUCKET_NAME) {

  }

  private Map<String, Object> executeSnowflake(String s) {

    return null;
  }

  @Test
  public void testS3ToSnowflakeIntegration() throws Exception {
//
//
//
//    // 1. Repopulate S3 bucket (e.g., upload a file)
//    repopulateS3Bucket();
//
//    // 2. Connect to Snowflake
//    Properties props = new Properties();
//    props.put("user", "your_username");
//    props.put("password", "your_password");
//    props.put("account", "your_account");
//    String snowflakeUrl = "jdbc:snowflake://youraccount.snowflakecomputing.com/";
//
//    try (Connection conn = DriverManager.getConnection(snowflakeUrl, props);
//        Statement stmt = conn.createStatement()) {
//
//      // 3. Execute SQL from file
//      executeSqlScript(stmt, "snowflake-schema.sql");
//
//      // 4. Execute a GraphQL query and compare the result
//      executeAndAssertGraphQL();
//    }
  }

  private void emptyS3Bucket(String BUCKET_NAME) {
    List<ObjectIdentifier> toDelete = s3Client.listObjectsV2Paginator(
            ListObjectsV2Request.builder().bucket(this.BUCKET_NAME).build())
        .stream()
        .flatMap(r -> r.contents().stream())
        .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
        .collect(Collectors.toList());

    if (!toDelete.isEmpty()) {
      s3Client.deleteObjects(DeleteObjectsRequest.builder()
          .bucket(this.BUCKET_NAME)
          .delete(Delete.builder().objects(toDelete).build())
          .build());
    }
  }

  private void repopulateS3Bucket() {
    // Example: Upload a file to S3
    // Assumes `RequestBody` and `PutObjectRequest` are correctly imported.
    s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key("example.txt").build(),
        RequestBody.fromString("This is a test file"));
  }

  private void executeSqlScript(Statement stmt, String scriptPath) throws Exception {
    try (BufferedReader reader = new BufferedReader(new FileReader(scriptPath))) {
      String line;
      StringBuilder sqlQuery = new StringBuilder();
      while ((line = reader.readLine()) != null) {
        if (!line.trim().isEmpty()) {
          sqlQuery.append(line);
          if (line.trim().endsWith(";")) {
            stmt.executeUpdate(sqlQuery.toString());
            sqlQuery = new StringBuilder();
          }
        }
      }
    }
  }

  private void executeAndAssertGraphQL() {
    // Example GraphQL client usage
    // Assumes `GraphQLClient`, `GraphQLResponse`, and `Assertions` are correctly imported.
//    GraphQLClient client = new GraphQLClient("http://your-graphql-endpoint");
//    String query = "{ yourQuery }";
//    GraphQLResponse response = client.executeQuery(query);
//    Assertions.assertEquals("expectedResult", response.getData());
  }
}

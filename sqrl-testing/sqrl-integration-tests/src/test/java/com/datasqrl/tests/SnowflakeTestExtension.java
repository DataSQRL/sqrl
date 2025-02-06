package com.datasqrl.tests;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class SnowflakeTestExtension implements TestExtension {

  Region region = Region.US_EAST_1;
  String databaseName = "mydatabase";

  public void eraseGlue() {

    var glueClient = GlueClient.builder()
        .region(region)
        .credentialsProvider(ProfileCredentialsProvider.create())
        .build();

    try {
      // List all tables in the database
      var getTablesRequest = GetTablesRequest.builder()
          .databaseName(databaseName)
          .build();

      var getTablesResponse = glueClient.getTables(getTablesRequest);

      // Delete each table
      for (Table table : getTablesResponse.tableList()) {
        var deleteTableRequest = DeleteTableRequest.builder()
            .databaseName(databaseName)
            .name(table.name())
            .build();

        glueClient.deleteTable(deleteTableRequest);
        System.out.println("Deleted table: " + table.name());
      }

      System.out.println("All tables deleted from the Glue database: " + databaseName);

    } catch (GlueException e) {
      System.err.println("Failed to delete tables: " + e.awsErrorDetails().errorMessage());
      e.printStackTrace();
    } finally {
      glueClient.close();
    }
  }

  public void eraseS3() {
    var bucketName = "daniel-iceberg-table-test";

    var region = Region.US_EAST_1; // Change the region if necessary
    var s3 = S3Client.builder()
        .region(region)
        .credentialsProvider(ProfileCredentialsProvider.create())
        .build();

    // List all objects in the bucket
    var listObjectsReq = ListObjectsV2Request.builder()
        .bucket(bucketName)
        .build();

    ListObjectsV2Response listObjectsRes;

    do {
      listObjectsRes = s3.listObjectsV2(listObjectsReq);

      for (S3Object s3Object : listObjectsRes.contents()) {
        // Delete each object
        var deleteObjectReq = DeleteObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Object.key())
            .build();

        s3.deleteObject(deleteObjectReq);
        System.out.println("Deleted: " + s3Object.key());
      }

      // If there are more objects to delete, set the continuation token
      listObjectsReq = listObjectsReq.toBuilder()
          .continuationToken(listObjectsRes.nextContinuationToken())
          .build();

    } while (listObjectsRes.isTruncated());

    s3.close();
    System.out.println("All files deleted from the bucket: " + bucketName);
  }

  @Override
  public void teardown() {
    eraseGlue();
    eraseS3();
  }

  @Override
  public void setup() {
    eraseGlue();
    eraseS3();
  }
}
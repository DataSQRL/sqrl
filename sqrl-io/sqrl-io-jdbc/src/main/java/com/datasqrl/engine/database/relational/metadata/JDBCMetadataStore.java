/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.metadata;

import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.metadata.MetadataStore;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

/**
 * Stores meta data in a database table with a simple key->value structure where key is the
 * concatenation of all keys on a path and value is a BLOB containing the serialized (via Kryo)
 * object.
 * <p>
 * TODO: move to Jackson serialization for better visibility
 */
public class JDBCMetadataStore implements MetadataStore {

  public static final int MAX_KEY_LENGTH = 2 * 256 + 128;
  public static final int DELIMITER_CODEPOINT = 47;
  private static final String DELIMITER_STRING = Character.toString(DELIMITER_CODEPOINT);

  public static final String TABLE_NAME = "metadata";

  private final Connection connection;
  private final String dialect;
  private final Kryo kryo;

  public JDBCMetadataStore(JdbcDataSystemConnector config, Kryo kryo) {
    this.kryo = kryo;
    try {
      this.connection = DriverManager.getConnection(
          config.getUrl(),
          config.getUser(),
          config.getPassword()
      );
    } catch (SQLException e) {
      throw new RuntimeException("Could not execute SQL query", e);
    }

    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate(CREATE_TABLE.get(config.getDialect().toUpperCase()));
    } catch (SQLException e) {
      throw new RuntimeException("Could not execute SQL query", e);
    }
    dialect = config.getDialect();
  }

  public static final Map<String, String> CREATE_TABLE = Map.of(
      "H2", "CREATE TABLE IF NOT EXISTS `" + TABLE_NAME + "` (\n" +
          "data_key VARCHAR(" + MAX_KEY_LENGTH * 2 + ") NOT NULL,\n" + //Multiply by 2 for UTF
          "data_value BLOB NOT NULL,\n" +
          "PRIMARY KEY (data_key)\n" +
          ");",
      "POSTGRES", "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (\n" +
          "data_key VARCHAR(" + MAX_KEY_LENGTH * 2 + ") NOT NULL,\n" + //Multiply by 2 for UTF
          "data_value bytea NOT NULL,\n" +
          "PRIMARY KEY (data_key)\n" +
          ");",
      "SQLITE", "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (\n" +
          "data_key VARCHAR(" + MAX_KEY_LENGTH * 2 + ") NOT NULL,\n" + //Multiply by 2 for UTF
          "data_value bytea NOT NULL,\n" +
          "PRIMARY KEY (data_key)\n" +
          ");"
  );

  public static final Map<String, String> UPSERT_QUERIES =
      ImmutableMap.of(
          "H2", "MERGE INTO `" + TABLE_NAME + "` " +
              "KEY ( data_key ) VALUES ( ?, ? );",
          "POSTGRES", "INSERT INTO " + TABLE_NAME + " " +
              "( data_key, data_value ) VALUES ( ?, ? ) ON CONFLICT ( data_key ) DO UPDATE SET data_value = EXCLUDED.data_value;",
          "SQLITE", "INSERT INTO " + TABLE_NAME + " " +
              "( data_key, data_value ) VALUES ( ?, ? ) ON CONFLICT ( data_key ) DO UPDATE SET data_value = EXCLUDED.data_value;",
          "MYSQL", "REPLACE INTO " + TABLE_NAME + " " +
              "( data_key, data_value ) VALUES ( ?, ? );"
      );

  public static final String GET_VALUE = "SELECT data_value FROM " + TABLE_NAME + " WHERE data_key = ?";

  public static final String DELETE_VALUE = "DELETE FROM " + TABLE_NAME + " WHERE data_key = ?";

  public static final String KEY_PREFIX = "SELECT data_key FROM " + TABLE_NAME + " WHERE data_key LIKE ?";

  @Override
  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeException("Error while closing database connection in Metadata store", e);
    }
  }

  private String getKeyString(@NonNull String firstKey, String... moreKeys) {
    String[] keys = new String[1 + moreKeys.length];
    keys[0] = firstKey;
    System.arraycopy(moreKeys, 0, keys, 1, moreKeys.length);
    return getKeyString(keys);
  }

  private String getKeyString(String... keys) {
    for (String key : keys) {
      Preconditions.checkArgument(
          StringUtils.isNotEmpty(key) && key.indexOf(DELIMITER_CODEPOINT) < 0, "Invalid key: %s",
          key);
    }
    String keyStr = String.join(DELIMITER_STRING, keys);
    Preconditions.checkArgument(keyStr.length() < MAX_KEY_LENGTH, "Key string is too long: %s",
        keyStr);
    return keyStr;
  }


  @Override
  public <T> void put(T value, String firstKey, String... moreKeys) {
    String keyStr = getKeyString(firstKey, moreKeys);
    String query = UPSERT_QUERIES.get(dialect.toUpperCase());
    Preconditions.checkArgument(query != null, "Dialect not supported: %s", dialect);

    byte[] data;
    try {
      ByteArrayOutputStream outstream = new ByteArrayOutputStream();
      Output out = new Output(outstream);
      kryo.writeClassAndObject(out, value);
      out.close();
      outstream.close();
      data = outstream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Exception serializing object", e);
    }

    try (PreparedStatement pstmt = connection.prepareStatement(query)) {
      pstmt.setString(1, keyStr);
      pstmt.setBytes(2, data);
      pstmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("Could not execute SQL query", e);
    }
  }

  @Override
  public boolean remove(String firstKey, String... moreKeys) {
    String keyStr = getKeyString(firstKey, moreKeys);

    try (PreparedStatement pstmt = connection.prepareStatement(DELETE_VALUE)) {
      pstmt.setString(1, keyStr);
      int number = pstmt.executeUpdate();
      return number > 0;
    } catch (SQLException e) {
      throw new RuntimeException("Could not execute SQL query", e);
    }
  }

  @Override
  public <T> T get(Class<T> clazz, String firstKey, String... moreKeys) {
    String keyStr = getKeyString(firstKey, moreKeys);

    try (PreparedStatement pstmt = connection.prepareStatement(GET_VALUE)) {
      pstmt.setString(1, keyStr);
      ResultSet rs = pstmt.executeQuery();
      if (rs.next()) {
        InputStream input = rs.getBinaryStream(1);
        return (T) kryo.readClassAndObject(new Input(input));
      } else {
        return null;
      }
    } catch (SQLException e) {
      throw new RuntimeException("Could not execute SQL query", e);
    }
  }

  @Override
  public Set<String> getSubKeys(String... keys) {
    String keyPrefix = "";
    if (keys.length > 0) {
      keyPrefix = getKeyString(keys) + DELIMITER_STRING;
    }

    Set<String> results = new HashSet<>();
    try (PreparedStatement pstmt = connection.prepareStatement(KEY_PREFIX)) {
      pstmt.setString(1, keyPrefix + "%"); // % is SQL special char for prefix in LIKE clause
      ResultSet rs = pstmt.executeQuery();
      while (rs.next()) {
        String key = rs.getString(1);
        Preconditions.checkArgument(key.startsWith(keyPrefix));
        String suffix = key.substring(keyPrefix.length());
        int nextDot = suffix.indexOf(DELIMITER_CODEPOINT);
        if (nextDot > 0) {
          suffix = suffix.substring(0, nextDot);
        }
        results.add(suffix);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Could not execute SQL query", e);
    }
    return results;
  }

}

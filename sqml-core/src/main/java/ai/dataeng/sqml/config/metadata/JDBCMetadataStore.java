package ai.dataeng.sqml.config.metadata;

import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.config.provider.JDBCConnectionProvider;
import ai.dataeng.sqml.config.provider.MetadataStoreProvider;
import ai.dataeng.sqml.tree.name.Name;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Stores meta data in a database table with a simple key->value structure
 * where key is the concatenation of all keys on a path and value is a BLOB containing the
 * serialized (via Kryo) object.
 *
 * TODO: move to Jackson serialization for better visibility
 */
public class JDBCMetadataStore implements MetadataStore {

    public static final int MAX_KEY_LENGTH = 2*256+128;
    public static final int DELIMITER_CODEPOINT = 46;
    private static final String DELIMITER_STRING = Character.toString(DELIMITER_CODEPOINT);

    public static final String TABLE_NAME = "metadata";

    private final Connection connection;
    private final JDBCConfiguration.Dialect dialect;
    private final Kryo kryo = new Kryo();


    public JDBCMetadataStore(JDBCConnectionProvider jdbcProvider) {
        try {
            this.connection = jdbcProvider.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Could not execute SQL query",e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load database driver",e);
        }

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(CREATE_TABLE);
        } catch (SQLException e) {
            throw new RuntimeException("Could not execute SQL query",e);
        }
        dialect = jdbcProvider.getDialect();
    }

    public static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS `"+TABLE_NAME+"` (\n" +
            "key VARCHAR("+MAX_KEY_LENGTH*2+") NOT NULL,\n" + //Multiply by 2 for UTF
            "value BLOB NOT NULL,\n" +
            "PRIMARY KEY (`key`)\n" +
            ");";

    public static final Map<JDBCConfiguration.Dialect,String> UPSERT_QUERIES =
            ImmutableMap.of(
                    JDBCConfiguration.Dialect.H2, "MERGE INTO `"+TABLE_NAME+"` " +
                            "KEY ( key ) VALUES ( ?, ? );",
                    JDBCConfiguration.Dialect.POSTGRES, "INSERT INTO `"+TABLE_NAME+"` " +
                            "( key, value ) VALUES ( ?, ? ) ON CONFLICT ( key ) DO UPDATE SET value = EXCLUDED.value;",
                    JDBCConfiguration.Dialect.MYSQL, "REPLACE INTO `"+TABLE_NAME+"` " +
                            "( key, value ) VALUES ( ?, ? );"
            );

    public static final String GET_VALUE = "SELECT value FROM `"+TABLE_NAME+"` WHERE key = ?";

    public static final String KEY_PREFIX = "SELECT key FROM `"+TABLE_NAME+"` WHERE key LIKE '?'";

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("Error while closing database connection in Metadata store", e);
        }
    }

    private String getKeyString(@NonNull String firstKey, String... moreKeys) {
        String[] keys = new String[1+ moreKeys.length];
        keys[0]=firstKey;
        System.arraycopy(moreKeys,0,keys,1,moreKeys.length);
        return getKeyString(keys);
    }

    private String getKeyString(String... keys) {
        for (String key : keys) {
            Preconditions.checkArgument(Name.validName(key),"Invalid key: %s",key);
        }
        String keyStr = String.join(DELIMITER_STRING,keys);
        Preconditions.checkArgument(keyStr.length()<MAX_KEY_LENGTH,"Key string is too long: %s",keyStr);
        return keyStr;
    }



    @Override
    public <T> void put(T value, String firstKey, String... moreKeys) {
        String keyStr = getKeyString(firstKey,moreKeys);
        String query = UPSERT_QUERIES.get(dialect);
        Preconditions.checkArgument(query!=null,"Dialect not supported: %s", dialect);
        Blob valueBlob;
        try {
            valueBlob= connection.createBlob();
        } catch (SQLException e) {
            throw new RuntimeException("Could not create blob",e);
        }
        try (PreparedStatement pstmt = connection.prepareStatement(query);
             OutputStream outputStream = valueBlob.setBinaryStream(1);
             Output out = new Output(outputStream)) {

            kryo.writeObject(out, value);
            pstmt.setString(1, keyStr);
            pstmt.setBlob(2, valueBlob);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Could not execute SQL query",e);
        } catch (IOException e) {
            throw new RuntimeException("Exception serializing object",e);
        }
    }

    @Override
    public <T> T get(Class<T> clazz, String firstKey, String... moreKeys) {
        String keyStr = getKeyString(firstKey,moreKeys);

        Blob valueBlob = null;

        try (PreparedStatement pstmt = connection.prepareStatement(GET_VALUE)) {
            pstmt.setString(1,keyStr);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                valueBlob = rs.getBlob(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not execute SQL query",e);
        }

        if (valueBlob==null) return null;
        else {
            try (Input in = new Input(valueBlob.getBinaryStream())) {
                return kryo.readObject(in, clazz);
            } catch (SQLException e) {
                throw new RuntimeException("Could not read blob",e);
            }
        }
    }

    @Override
    public Set<String> getSubKeys(String... keys) {
        String keyPrefix = "";
        if (keys.length>0) keyPrefix = getKeyString(keys) + DELIMITER_STRING;

        Set<String> results = new HashSet<>();
        try (PreparedStatement pstmt = connection.prepareStatement(KEY_PREFIX)) {
            pstmt.setString(1,keyPrefix + "%"); // % is SQL special char for prefix in LIKE clause
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String key = rs.getString(1);
                Preconditions.checkArgument(key.startsWith(keyPrefix));
                String suffix = key.substring(keyPrefix.length(),key.length());
                int nextDot = suffix.indexOf(DELIMITER_CODEPOINT);
                if (nextDot>0) suffix = suffix.substring(0,nextDot);
                results.add(suffix);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not execute SQL query",e);
        }
        return results;
    }

    public static class Provider implements MetadataStoreProvider {

        @Override
        public MetadataStore openStore(JDBCConnectionProvider jdbc) {
            return new JDBCMetadataStore(jdbc);
        }

    }
}

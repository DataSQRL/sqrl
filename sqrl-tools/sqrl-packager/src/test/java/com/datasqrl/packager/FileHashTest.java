/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import com.datasqrl.packager.util.FileHash;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileHashTest {

    @Test
    @SneakyThrows
    public void testFileHash() {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("package-configtest.json");
        assertEquals("6798e72912ab81cac1b0a9b713175730", FileHash.getFor(is));
    }

    @Test
    @Disabled
    @SneakyThrows
    public void generateHash() {
        System.out.println(FileHash.getFor(Paths.get("/Users/matthias/git/sqrl-repository/testdata/8_e-WN-FzfckOREZ85JY6pv6ktQ.zip")));
    }

}

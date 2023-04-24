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
        assertEquals("f9d6ac76c70e216728c70dfcd8bf9398", FileHash.getFor(is));
    }

    @Test
    @Disabled
    @SneakyThrows
    public void generateHash() {
        System.out.println(FileHash.getFor(Paths.get("/Users/matthias/git/sqrl-repository/testdata/8_e-WN-FzfckOREZ85JY6pv6ktQ.zip")));
    }

}

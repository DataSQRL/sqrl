package com.datasqrl.packager;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class GeneratePackageIdTest {

    @Test
    public void generateIds() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 100000; i++) {
            String id = GeneratePackageId.generate();
            assertEquals(27,id.length());
            assertTrue(ids.add(id));
        }
    }

    @Test
    @Disabled
    public void generateSingleId() {
        System.out.println(GeneratePackageId.generate());
    }

}

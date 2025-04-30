package com.datasqrl.error;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ResourceFileUtil {

    public static String readResourceFileContents(String resourcePath) {
        var contentBuilder = new StringBuilder();

        try (var inputStream = ResourceFileUtil.class.getResourceAsStream(resourcePath);
             var reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                contentBuilder.append(currentLine).append("\n");
            }
        } catch (IOException | NullPointerException e) {
            throw new IllegalArgumentException("Unable to read resource file: " + resourcePath, e);
        }
        return contentBuilder.toString();
    }

    public static void main(String[] args) {
        var resourcePath = "/errorCodes/cannot_resolve_tablesink.md";
        var fileContent = readResourceFileContents(resourcePath);
        System.out.println(fileContent);
    }
}
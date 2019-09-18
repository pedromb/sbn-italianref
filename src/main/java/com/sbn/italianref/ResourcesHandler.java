package com.sbn.italianref;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ResourcesHandler {

    public ResourcesHandler() {};

    public Path getDirFromResources(String fileName) {

        ClassLoader classLoader = getClass().getClassLoader();

        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file is not found!");
        } else {
            return Paths.get(resource.getFile());
        }

    }
}

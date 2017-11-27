package edu.tntech.graph.helper;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

public class Helper {
    private static Helper instance = null;

    public static Helper getInstance() {
        if (instance == null) {
            instance = new Helper();
        }
        return instance;
    }

    public String getAbsolutePath(String file) {
        String path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        int jarIndex = path.lastIndexOf(File.separator);
        return path.substring(0, jarIndex) + File.separator + file;
    }

    public double getContinuousUniformRandomNumber(Integer min, Integer max) {
        return ThreadLocalRandom.current().nextDouble(min, max);
    }
}

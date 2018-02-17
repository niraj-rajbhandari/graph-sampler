package edu.tntech.graph.helper;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

import edu.tntech.graph.enumerator.FileType;
import edu.tntech.graph.enumerator.LogLevel;

public class Helper {
    private static final String DATA_DIRECTORY = "data";
    private static final String GRAPH_DIRECTORY = "graph";
    private static final String OUTPUT_DIRECTORY = "output";
    private static final String LOG_LEVEL_KEY = "log-level";
    private static final String DATA_TYPE_KEY = "data-type";
    private static Helper instance = null;

    public static Helper getInstance() {
        if (instance == null) {
            instance = new Helper();
        }
        return instance;
    }

    public String getAbsolutePath(String file, FileType fileType) {
        String path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        int jarIndex = path.lastIndexOf(File.separator);
        switch (fileType) {
            case DATA:
                return path.substring(0, jarIndex) + File.separator + DATA_DIRECTORY + File.separator +
                        DATA_TYPE_KEY + File.separator + file;
            case GRAPH:
                return path.substring(0, jarIndex) + File.separator + GRAPH_DIRECTORY + File.separator +
                        DATA_TYPE_KEY + File.separator + file;
            case OUTPUT:
                return path.substring(0, jarIndex) + File.separator + OUTPUT_DIRECTORY + File.separator +
                        DATA_TYPE_KEY + File.separator + file;
            default:
                return path.substring(0, jarIndex) + File.separator + file;
        }
    }

    public double getContinuousUniformRandomNumber(Integer min, Integer max) {
        return ThreadLocalRandom.current().nextDouble(min, max);
    }

    public Level getLogLevel(ConfigReader config) {
        String configuredLevel = config.getProperty(LOG_LEVEL_KEY);
        if (configuredLevel.equals(LogLevel.DEBUG.getlevel())) {
            return Level.FINE;
        } else if (configuredLevel.equals(LogLevel.INFO.getlevel())) {
            return Level.INFO;
        } else {
            return Level.SEVERE;
        }
    }

    /**
     * Gets CPU time in nanoseconds
     *
     * @return
     */
    public static long getCpuTime() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        return bean.isCurrentThreadCpuTimeSupported() ?
                bean.getCurrentThreadCpuTime() : 0L;
    }
}

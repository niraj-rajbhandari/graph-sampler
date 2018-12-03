package edu.tntech.graph.helper;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;

import edu.tntech.graph.enumerator.FileType;
import edu.tntech.graph.enumerator.LogLevel;
import edu.tntech.graph.sampler.Sampler;

public class Helper {
    private static final String DATA_DIRECTORY = "data";
    private static final String GRAPH_DIRECTORY = "graph";
    private static final String OUTPUT_DIRECTORY = "output";
    private static final String LOG_LEVEL_KEY = "log-level";
    private static final Integer NANOSECONDS_IN_SECOND = 1000000000;

    public static final String DATA_TYPE_KEY = "data-type";


    private static Helper instance = null;

    public static Helper getInstance() {
        if (instance == null) {
            instance = new Helper();
        }
        return instance;
    }

    public String getAbsolutePath(String file, String dataType, FileType fileType) {
        String path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        int jarIndex = path.lastIndexOf(File.separator);
        String absolutePath;
        switch (fileType) {
            case DATA:
                absolutePath = path.substring(0, jarIndex) + File.separator + DATA_DIRECTORY + File.separator +
                        dataType + File.separator + file;
                break;
            case GRAPH:
                absolutePath = path.substring(0, jarIndex) + File.separator + GRAPH_DIRECTORY + File.separator +
                        dataType + File.separator + file;
                break;
            case OUTPUT:
                absolutePath = path.substring(0, jarIndex) + File.separator + OUTPUT_DIRECTORY + File.separator +
                        dataType + File.separator + file;
                break;
            default:
                absolutePath = path.substring(0, jarIndex) + File.separator + file;
                break;
        }

        return absolutePath;
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

    public static double getStandardDeviation(List<Integer> values, double mean) {
        double sum = values.stream().map(v -> Math.pow(v - mean, 2)).mapToDouble(Double::doubleValue).sum();
        if (values.size() > 1) {
            double variance = sum / (values.size() - 1);
            return Math.sqrt(variance);
        } else {
            return 0;
        }
    }

    public static double getMean(List<Integer> values) {
        int sum = values.stream().mapToInt(Integer::intValue).sum();
        return sum / (double) values.size();
    }

    public static void makeDirectory(String path, boolean deleteOnExist) {
        File directory = new File(path);
        if (deleteOnExist)
            directory.deleteOnExit();

        directory.mkdirs();
    }

    public static void writeCPUTimeToFile(int window, String dataType) throws IOException {
        long cpuTimeNano = getCpuTime();
        double cpuTimeInSeconds = (double) cpuTimeNano / NANOSECONDS_IN_SECOND;
        String absolutePathToDirectory =
                Helper.getInstance().getAbsolutePath(Sampler.GRAPH_SAMPLER, dataType, FileType.OUTPUT);
        Helper.makeDirectory(absolutePathToDirectory, true);
        String cpuTimeFile = absolutePathToDirectory + File.separator + Sampler.GRAPH_SAMPLER + "-" + window + ".txt";
        try (RandomAccessFile fileStream = new RandomAccessFile(cpuTimeFile, "rw");
             FileChannel channel = fileStream.getChannel()) {
            String content =
                    "CPU Time in (ns): " + cpuTimeNano + "\n" + "CPU Time in seconds: " + cpuTimeInSeconds + "\n";

            byte[] contentBytes = content.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(contentBytes.length);
            buffer.put(contentBytes);
            buffer.flip();
            channel.write(buffer);
        }
    }

    public static void rename(String oldFileName, String directory, String newFileName) throws IOException {
        String fileToRename = directory  + oldFileName;
        File file = new File(fileToRename);
        if (file.exists()) {
            Path filePath = Paths.get(fileToRename);
            Path targetPath = Paths.get(directory);
            Files.move(filePath,targetPath.resolve(newFileName));
        }
    }
    public static Logger getLogger(String className, String dataType){
        Logger logger = Logger.getLogger(className);
        FileHandler fileHandler;
        try{
            logger.setUseParentHandlers(false);
            logger.setLevel(Level.FINE);
            String logFile = getInstance().getAbsolutePath("graph_sampler.txt", dataType,FileType.OUTPUT);
            fileHandler = new FileHandler(logFile);
            logger.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        }catch(SecurityException | IOException e){
            e.printStackTrace();
        }
        return logger;
    }
}

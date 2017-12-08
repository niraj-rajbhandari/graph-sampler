package edu.tntech.graph.helper;

import edu.tntech.graph.enumerator.FileType;
import edu.tntech.graph.exception.PropertyNotFoundException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigReader {

    public static final String CONFIG_FILE = "config.properties";

    private Properties properties;

    private static ConfigReader configReader = null;

    ConfigReader() throws FileNotFoundException {
        Helper helper = Helper.getInstance();
        try (InputStream inputStream = new FileInputStream(helper.getAbsolutePath(CONFIG_FILE, FileType.OTHERS))) {
            this.properties = new Properties();
            if (inputStream == null) {
                throw new FileNotFoundException("Config File not found");
            }
            this.properties.load(inputStream);
        }catch(IOException e){
            throw new FileNotFoundException(e.getMessage());
        }
    }

    public String getProperty(String key) throws PropertyNotFoundException {
        String config = this.properties.getProperty(key);
        if (config == null) {
            throw new PropertyNotFoundException("Property: " + key + " not found");
        }
        return config;
    }

    public static ConfigReader getInstance() throws FileNotFoundException {
        if (configReader == null) {
            configReader = new ConfigReader();
        }
        return configReader;
    }

}


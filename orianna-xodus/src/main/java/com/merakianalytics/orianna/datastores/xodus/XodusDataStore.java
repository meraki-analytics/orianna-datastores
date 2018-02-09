package com.merakianalytics.orianna.datastores.xodus;

import java.nio.file.Paths;

import com.merakianalytics.datapipelines.AbstractDataStore;

import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;

public abstract class XodusDataStore extends AbstractDataStore implements AutoCloseable {
    public static class Configuration {
        private String dataDirectory = Paths.get(System.getProperty("user.home"), ".orianna", "xodus").toString();

        /**
         * @return the dataDirectory
         */
        public String getDataDirectory() {
            return dataDirectory;
        }

        /**
         * @param dataDirectory
         *        the dataDirectory to set
         */
        public void setDataDirectory(final String dataDirectory) {
            this.dataDirectory = dataDirectory;
        }
    }

    protected final Environment xodus;

    public XodusDataStore(final Configuration config) {
        xodus = Environments.newInstance(config.getDataDirectory());
    }

    @Override
    public void close() {
        xodus.close();
    }
}

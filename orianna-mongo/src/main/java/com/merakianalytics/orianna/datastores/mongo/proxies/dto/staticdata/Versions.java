package com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata;

import java.lang.reflect.Field;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.orianna.types.common.OriannaException;
import com.merakianalytics.orianna.types.dto.DataObject;

public class Versions {
    private static final Field DATA = getDataField();
    private static Logger LOGGER = LoggerFactory.getLogger(Versions.class);

    @SuppressWarnings("unchecked")
    public static Versions convert(final com.merakianalytics.orianna.types.dto.staticdata.Versions versions) {
        final Versions proxy = new Versions();
        proxy.setPlatform(versions.getPlatform());
        try {
            proxy.setData((List<String>)DATA.get(versions));
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to get data from Versions! Report this to the orianna team!", e);
            throw new OriannaException("Failed to get data from Versions! Report this to the orianna team!", e);
        }
        return proxy;
    }

    private static Field getDataField() {
        try {
            final Field data = DataObject.ListProxy.class.getDeclaredField("data");
            data.setAccessible(true);
            return data;
        } catch(NoSuchFieldException | SecurityException e) {
            LOGGER.error("Couldn't get ListProxy data field! Report this to the orianna team!", e);
            throw new OriannaException("Couldn't get ListProxy data field! Report this to the orianna team!", e);
        }
    }

    private List<String> data;
    private String platform;

    public com.merakianalytics.orianna.types.dto.staticdata.Versions convert() {
        final com.merakianalytics.orianna.types.dto.staticdata.Versions languages =
            new com.merakianalytics.orianna.types.dto.staticdata.Versions(0);
        languages.setPlatform(platform);
        try {
            DATA.set(languages, data);
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to set data on Versions! Report this to the orianna team!", e);
            throw new OriannaException("Failed to set data on Versions! Report this to the orianna team!", e);
        }
        return languages;
    }

    /**
     * @return the data
     */
    public List<String> getData() {
        return data;
    }

    /**
     * @return the platform
     */
    public String getPlatform() {
        return platform;
    }

    /**
     * @param data
     *        the data to set
     */
    public void setData(final List<String> data) {
        this.data = data;
    }

    /**
     * @param platform
     *        the platform to set
     */
    public void setPlatform(final String platform) {
        this.platform = platform;
    }
}

package com.merakianalytics.orianna.datastores.mongo.proxies.dto.league;

import java.lang.reflect.Field;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.orianna.types.common.OriannaException;
import com.merakianalytics.orianna.types.dto.DataObject;

public class PositionalQueuesList {
    private static final Field DATA = getDataField();
    private static Logger LOGGER = LoggerFactory.getLogger(PositionalQueuesList.class);

    @SuppressWarnings("unchecked")
    public static PositionalQueuesList convert(final com.merakianalytics.orianna.types.dto.league.PositionalQueuesList queues) {
        final PositionalQueuesList proxy = new PositionalQueuesList();
        proxy.setPlatform(queues.getPlatform());
        try {
            proxy.setData((List<String>)DATA.get(queues));
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to get data from PositionalQueuesList! Report this to the orianna team!", e);
            throw new OriannaException("Failed to get data from PositionalQueuesList! Report this to the orianna team!", e);
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

    public com.merakianalytics.orianna.types.dto.league.PositionalQueuesList convert() {
        final com.merakianalytics.orianna.types.dto.league.PositionalQueuesList languages =
            new com.merakianalytics.orianna.types.dto.league.PositionalQueuesList(0);
        languages.setPlatform(platform);
        try {
            DATA.set(languages, data);
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to set data on PositionalQueuesList! Report this to the orianna team!", e);
            throw new OriannaException("Failed to set data on PositionalQueuesList! Report this to the orianna team!", e);
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

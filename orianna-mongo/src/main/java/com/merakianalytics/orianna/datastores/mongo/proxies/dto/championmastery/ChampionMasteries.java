package com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery;

import java.lang.reflect.Field;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.orianna.types.common.OriannaException;
import com.merakianalytics.orianna.types.dto.DataObject;
import com.merakianalytics.orianna.types.dto.championmastery.ChampionMastery;

public class ChampionMasteries {
    private static final Field DATA = getDataField();
    private static Logger LOGGER = LoggerFactory.getLogger(ChampionMasteries.class);

    @SuppressWarnings("unchecked")
    public static ChampionMasteries convert(final com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries masteries) {
        final ChampionMasteries proxy = new ChampionMasteries();
        proxy.setPlatform(masteries.getPlatform());
        proxy.setSummonerId(masteries.getSummonerId());
        try {
            proxy.setData((List<ChampionMastery>)DATA.get(masteries));
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to get data from ChampionMasteries! Report this to the orianna team!", e);
            throw new OriannaException("Failed to get data from ChampionMasteries! Report this to the orianna team!", e);
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
    private List<ChampionMastery> data;
    private String platform;

    private long summonerId;

    public ChampionMasteries() {

    }

    public com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries convert() {
        final com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries masteries =
            new com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries(0);
        masteries.setPlatform(platform);
        masteries.setSummonerId(summonerId);
        try {
            DATA.set(masteries, data);
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to set data on ChampionMasteries! Report this to the orianna team!", e);
            throw new OriannaException("Failed to set data on ChampionMasteries! Report this to the orianna team!", e);
        }
        return masteries;
    }

    /**
     * @return the data
     */
    public List<ChampionMastery> getData() {
        return data;
    }

    /**
     * @return the platform
     */
    public String getPlatform() {
        return platform;
    }

    /**
     * @return the summonerId
     */
    public long getSummonerId() {
        return summonerId;
    }

    /**
     * @param data
     *        the data to set
     */
    public void setData(final List<ChampionMastery> data) {
        this.data = data;
    }

    /**
     * @param platform
     *        the platform to set
     */
    public void setPlatform(final String platform) {
        this.platform = platform;
    }

    /**
     * @param summonerId
     *        the summonerId to set
     */
    public void setSummonerId(final long summonerId) {
        this.summonerId = summonerId;
    }
}

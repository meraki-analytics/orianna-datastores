package com.merakianalytics.orianna.datastores.mongo.proxies.dto.league;

import java.lang.reflect.Field;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.orianna.types.common.OriannaException;
import com.merakianalytics.orianna.types.dto.DataObject;
import com.merakianalytics.orianna.types.dto.league.LeaguePosition;

public class SummonerPositions {
    private static final Field DATA = getDataField();
    private static Logger LOGGER = LoggerFactory.getLogger(SummonerPositions.class);

    @SuppressWarnings("unchecked")
    public static SummonerPositions convert(final com.merakianalytics.orianna.types.dto.league.SummonerPositions positions) {
        final SummonerPositions proxy = new SummonerPositions();
        proxy.setPlatform(positions.getPlatform());
        proxy.setSummonerId(positions.getSummonerId());
        try {
            proxy.setData((Set<LeaguePosition>)DATA.get(positions));
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to get data from SummonerPositions! Report this to the orianna team!", e);
            throw new OriannaException("Failed to get data from SummonerPositions! Report this to the orianna team!", e);
        }
        return proxy;
    }

    private static Field getDataField() {
        try {
            final Field data = DataObject.SetProxy.class.getDeclaredField("data");
            data.setAccessible(true);
            return data;
        } catch(NoSuchFieldException | SecurityException e) {
            LOGGER.error("Couldn't get SetProxy data field! Report this to the orianna team!", e);
            throw new OriannaException("Couldn't get SetProxy data field! Report this to the orianna team!", e);
        }
    }

    private Set<LeaguePosition> data;
    private String platform;
    private long summonerId;

    public com.merakianalytics.orianna.types.dto.league.SummonerPositions convert() {
        final com.merakianalytics.orianna.types.dto.league.SummonerPositions positions =
            new com.merakianalytics.orianna.types.dto.league.SummonerPositions();
        positions.setPlatform(platform);
        positions.setSummonerId(summonerId);
        try {
            DATA.set(positions, data);
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to set data on SummonerPositions! Report this to the orianna team!", e);
            throw new OriannaException("Failed to set data on SummonerPositions! Report this to the orianna team!", e);
        }
        return positions;
    }

    /**
     * @return the data
     */
    public Set<LeaguePosition> getData() {
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
    public void setData(final Set<LeaguePosition> data) {
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

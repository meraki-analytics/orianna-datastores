package com.merakianalytics.orianna.datastores.mongo.proxies.dto.match;

import java.lang.reflect.Field;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.orianna.types.common.OriannaException;
import com.merakianalytics.orianna.types.dto.DataObject;

public class TournamentMatches {
    private static final Field DATA = getDataField();
    private static Logger LOGGER = LoggerFactory.getLogger(TournamentMatches.class);

    @SuppressWarnings("unchecked")
    public static TournamentMatches convert(final com.merakianalytics.orianna.types.dto.match.TournamentMatches matches) {
        final TournamentMatches proxy = new TournamentMatches();
        proxy.setPlatform(matches.getPlatform());
        proxy.setTournamentCode(matches.getTournamentCode());
        try {
            proxy.setData((List<Long>)DATA.get(matches));
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to get data from TournamentMatches! Report this to the orianna team!", e);
            throw new OriannaException("Failed to get data from TournamentMatches! Report this to the orianna team!", e);
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

    private List<Long> data;

    private String platform, tournamentCode;

    public com.merakianalytics.orianna.types.dto.match.TournamentMatches convert() {
        final com.merakianalytics.orianna.types.dto.match.TournamentMatches matches =
            new com.merakianalytics.orianna.types.dto.match.TournamentMatches(0);
        matches.setPlatform(platform);
        matches.setTournamentCode(tournamentCode);
        try {
            DATA.set(matches, data);
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to set data on TournamentMatches! Report this to the orianna team!", e);
            throw new OriannaException("Failed to set data on TournamentMatches! Report this to the orianna team!", e);
        }
        return matches;
    }

    /**
     * @return the data
     */
    public List<Long> getData() {
        return data;
    }

    /**
     * @return the platform
     */
    public String getPlatform() {
        return platform;
    }

    /**
     * @return the tournamentCode
     */
    public String getTournamentCode() {
        return tournamentCode;
    }

    /**
     * @param data
     *        the data to set
     */
    public void setData(final List<Long> data) {
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
     * @param tournamentCode
     *        the tournamentCode to set
     */
    public void setTournamentCode(final String tournamentCode) {
        this.tournamentCode = tournamentCode;
    }
}

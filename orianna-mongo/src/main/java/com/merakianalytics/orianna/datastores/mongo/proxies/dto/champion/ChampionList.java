package com.merakianalytics.orianna.datastores.mongo.proxies.dto.champion;

import java.util.ArrayList;

public class ChampionList {
    public static ChampionList convert(final com.merakianalytics.orianna.types.dto.champion.ChampionList list) {
        final ChampionList proxy = new ChampionList();
        proxy.setFreeToPlay(list.isFreeToPlay());
        proxy.setPlatform(list.getPlatform());
        return proxy;
    }

    private boolean freeToPlay;
    private String platform;

    public com.merakianalytics.orianna.types.dto.champion.ChampionList convert(final int initialCapacity) {
        final com.merakianalytics.orianna.types.dto.champion.ChampionList list =
            new com.merakianalytics.orianna.types.dto.champion.ChampionList();
        list.setChampions(new ArrayList<>(initialCapacity));
        list.setFreeToPlay(freeToPlay);
        list.setPlatform(platform);
        return list;
    }

    /**
     * @return the platform
     */
    public String getPlatform() {
        return platform;
    }

    /**
     * @return the freeToPlay
     */
    public boolean isFreeToPlay() {
        return freeToPlay;
    }

    /**
     * @param freeToPlay
     *        the freeToPlay to set
     */
    public void setFreeToPlay(final boolean freeToPlay) {
        this.freeToPlay = freeToPlay;
    }

    /**
     * @param platform
     *        the platform to set
     */
    public void setPlatform(final String platform) {
        this.platform = platform;
    }
}

package com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery;

public class ChampionMasteries {
    public static ChampionMasteries convert(final com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries masteries) {
        final ChampionMasteries proxy = new ChampionMasteries();
        proxy.setPlatform(masteries.getPlatform());
        proxy.setSummonerId(masteries.getSummonerId());
        return proxy;
    }

    private String platform, summonerId;

    public com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries convert(final int initialCapacity) {
        final com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries masteries =
            new com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries(initialCapacity);
        masteries.setPlatform(platform);
        masteries.setSummonerId(summonerId);
        return masteries;
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
    public String getSummonerId() {
        return summonerId;
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
    public void setSummonerId(final String summonerId) {
        this.summonerId = summonerId;
    }
}

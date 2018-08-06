package com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata;

import java.util.ArrayList;
import java.util.Map;

public class Patches {
    public static Patches convert(final com.merakianalytics.orianna.types.dto.staticdata.Patches data) {
        final Patches proxy = new Patches();
        proxy.setPlatform(data.getPlatform());
        proxy.setShifts(data.getShifts());
        return proxy;
    }

    private String platform;

    private Map<String, Long> shifts;

    public com.merakianalytics.orianna.types.dto.staticdata.Patches convert(final int initialCapacity) {
        final com.merakianalytics.orianna.types.dto.staticdata.Patches data = new com.merakianalytics.orianna.types.dto.staticdata.Patches();
        data.setPlatform(platform);
        data.setPatches(new ArrayList<>(initialCapacity));
        data.setShifts(shifts);
        return data;
    }

    /**
     * @return the platform
     */
    public String getPlatform() {
        return platform;
    }

    /**
     * @return the shifts
     */
    public Map<String, Long> getShifts() {
        return shifts;
    }

    /**
     * @param platform
     *        the platform to set
     */
    public void setPlatform(final String platform) {
        this.platform = platform;
    }

    /**
     * @param shifts
     *        the shifts to set
     */
    public void setShifts(final Map<String, Long> shifts) {
        this.shifts = shifts;
    }
}

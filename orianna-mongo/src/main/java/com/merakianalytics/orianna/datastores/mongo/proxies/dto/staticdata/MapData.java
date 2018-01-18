package com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata;

import java.util.HashMap;

public class MapData {
    public static MapData convert(final com.merakianalytics.orianna.types.dto.staticdata.MapData data) {
        final MapData proxy = new MapData();
        proxy.setLocale(data.getLocale());
        proxy.setPlatform(data.getPlatform());
        proxy.setType(data.getType());
        proxy.setVersion(data.getVersion());
        return proxy;
    }

    private String version, locale, platform, type;

    public com.merakianalytics.orianna.types.dto.staticdata.MapData convert(final int initialCapacity) {
        final com.merakianalytics.orianna.types.dto.staticdata.MapData data = new com.merakianalytics.orianna.types.dto.staticdata.MapData();
        data.setData(new HashMap<>(initialCapacity));
        data.setLocale(locale);
        data.setPlatform(platform);
        data.setType(type);
        data.setVersion(version);
        return data;
    }

    /**
     * @return the locale
     */
    public String getLocale() {
        return locale;
    }

    /**
     * @return the platform
     */
    public String getPlatform() {
        return platform;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * @param locale
     *        the locale to set
     */
    public void setLocale(final String locale) {
        this.locale = locale;
    }

    /**
     * @param platform
     *        the platform to set
     */
    public void setPlatform(final String platform) {
        this.platform = platform;
    }

    /**
     * @param type
     *        the type to set
     */
    public void setType(final String type) {
        this.type = type;
    }

    /**
     * @param version
     *        the version to set
     */
    public void setVersion(final String version) {
        this.version = version;
    }
}

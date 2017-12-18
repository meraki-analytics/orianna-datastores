package com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ChampionList {
    public static ChampionList convert(final com.merakianalytics.orianna.types.dto.staticdata.ChampionList list) {
        final ChampionList proxy = new ChampionList();
        proxy.setFormat(list.getFormat());
        proxy.setIncludedData(list.getIncludedData());
        proxy.setKeys(list.getKeys());
        proxy.setLocale(list.getLocale());
        proxy.setPlatform(list.getPlatform());
        proxy.setType(list.getType());
        proxy.setVersion(list.getVersion());
        return proxy;
    }

    private Set<String> includedData;
    private Map<String, String> keys;
    private String version, type, format, locale, platform;

    public com.merakianalytics.orianna.types.dto.staticdata.ChampionList convert(final int initialCapacity, final boolean dataById) {
        final com.merakianalytics.orianna.types.dto.staticdata.ChampionList list = new com.merakianalytics.orianna.types.dto.staticdata.ChampionList();
        list.setData(new HashMap<>(initialCapacity));
        list.setDataById(dataById);
        list.setFormat(format);
        list.setIncludedData(includedData);
        list.setKeys(keys);
        list.setLocale(locale);
        list.setPlatform(platform);
        list.setType(type);
        list.setVersion(version);
        return list;
    }

    /**
     * @return the format
     */
    public String getFormat() {
        return format;
    }

    /**
     * @return the includedData
     */
    public Set<String> getIncludedData() {
        return includedData;
    }

    /**
     * @return the keys
     */
    public Map<String, String> getKeys() {
        return keys;
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
     * @param format
     *        the format to set
     */
    public void setFormat(final String format) {
        this.format = format;
    }

    /**
     * @param includedData
     *        the includedData to set
     */
    public void setIncludedData(final Set<String> includedData) {
        this.includedData = includedData;
    }

    /**
     * @param keys
     *        the keys to set
     */
    public void setKeys(final Map<String, String> keys) {
        this.keys = keys;
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

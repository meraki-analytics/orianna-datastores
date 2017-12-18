package com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata;

import java.util.HashMap;
import java.util.Set;

import com.merakianalytics.orianna.types.dto.staticdata.MasteryTree;

public class MasteryList {
    public static MasteryList convert(final com.merakianalytics.orianna.types.dto.staticdata.MasteryList list) {
        final MasteryList proxy = new MasteryList();
        proxy.setIncludedData(list.getIncludedData());
        proxy.setLocale(list.getLocale());
        proxy.setPlatform(list.getPlatform());
        proxy.setTree(list.getTree());
        proxy.setType(list.getType());
        proxy.setVersion(list.getVersion());
        return proxy;
    }

    private Set<String> includedData;
    private MasteryTree tree;
    private String version, locale, platform, type;

    public com.merakianalytics.orianna.types.dto.staticdata.MasteryList convert(final int initialCapacity) {
        final com.merakianalytics.orianna.types.dto.staticdata.MasteryList list = new com.merakianalytics.orianna.types.dto.staticdata.MasteryList();
        list.setData(new HashMap<>(initialCapacity));
        list.setIncludedData(includedData);
        list.setLocale(locale);
        list.setPlatform(platform);
        list.setTree(tree);
        list.setType(type);
        list.setVersion(version);
        return list;
    }

    /**
     * @return the includedData
     */
    public Set<String> getIncludedData() {
        return includedData;
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
     * @return the tree
     */
    public MasteryTree getTree() {
        return tree;
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
     * @param includedData
     *        the includedData to set
     */
    public void setIncludedData(final Set<String> includedData) {
        this.includedData = includedData;
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
     * @param tree
     *        the tree to set
     */
    public void setTree(final MasteryTree tree) {
        this.tree = tree;
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

package com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.merakianalytics.orianna.types.dto.staticdata.Group;
import com.merakianalytics.orianna.types.dto.staticdata.ItemTree;

public class ItemList {
    public static ItemList convert(final com.merakianalytics.orianna.types.dto.staticdata.ItemList list) {
        final ItemList proxy = new ItemList();
        proxy.setGroups(list.getGroups());
        proxy.setIncludedData(list.getIncludedData());
        proxy.setLocale(list.getLocale());
        proxy.setPlatform(list.getPlatform());
        proxy.setTree(list.getTree());
        proxy.setType(list.getType());
        proxy.setVersion(list.getVersion());
        return proxy;
    }

    private List<Group> groups;
    private Set<String> includedData;
    private List<ItemTree> tree;
    private String version, locale, platform, type;

    public com.merakianalytics.orianna.types.dto.staticdata.ItemList convert(final int initialCapacity) {
        final com.merakianalytics.orianna.types.dto.staticdata.ItemList list = new com.merakianalytics.orianna.types.dto.staticdata.ItemList();
        list.setData(new HashMap<>(initialCapacity));
        list.setGroups(groups);
        list.setIncludedData(includedData);
        list.setLocale(locale);
        list.setPlatform(platform);
        list.setTree(tree);
        list.setType(type);
        list.setVersion(version);
        return list;
    }

    /**
     * @return the groups
     */
    public List<Group> getGroups() {
        return groups;
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
    public List<ItemTree> getTree() {
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
     * @param groups
     *        the groups to set
     */
    public void setGroups(final List<Group> groups) {
        this.groups = groups;
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
    public void setTree(final List<ItemTree> tree) {
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

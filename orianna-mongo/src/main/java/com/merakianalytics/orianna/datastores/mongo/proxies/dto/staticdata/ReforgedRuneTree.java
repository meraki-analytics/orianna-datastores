package com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.orianna.types.common.OriannaException;
import com.merakianalytics.orianna.types.dto.DataObject;
import com.merakianalytics.orianna.types.dto.staticdata.ReforgedRunePath;
import com.merakianalytics.orianna.types.dto.staticdata.ReforgedRuneSlot;

public class ReforgedRuneTree {
    private static final Field DATA = getDataField();
    private static Logger LOGGER = LoggerFactory.getLogger(ReforgedRuneTree.class);

    @SuppressWarnings("unchecked")
    public static ReforgedRuneTree convert(final com.merakianalytics.orianna.types.dto.staticdata.ReforgedRuneTree tree) {
        final ReforgedRuneTree proxy = new ReforgedRuneTree();
        try {
            proxy.setData(new ArrayList<>((List<ReforgedRunePath>)DATA.get(tree)));
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to get data from ReforgedRuneTree! Report this to the orianna team!", e);
            throw new OriannaException("Failed to get data from ReforgedRuneTree! Report this to the orianna team!", e);
        }
        for(int i = 0; i < proxy.getData().size(); i++) {
            final ReforgedRunePath path = proxy.getData().get(i);
            final ReforgedRunePath newPath = new ReforgedRunePath();
            proxy.getData().set(i, newPath);
            newPath.setIcon(path.getIcon());
            newPath.setId(path.getId());
            newPath.setKey(path.getKey());
            newPath.setName(path.getName());
            newPath.setSlots(new ArrayList<>(path.getSlots()));
            for(int j = 0; j < newPath.getSlots().size(); j++) {
                final ReforgedRuneSlot newSlot = new ReforgedRuneSlot();
                newPath.getSlots().set(j, newSlot);
                newSlot.setRunes(new ArrayList<>(0));
            }
        }
        proxy.setPlatform(tree.getPlatform());
        proxy.setLocale(tree.getLocale());
        proxy.setVersion(tree.getVersion());
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

    private List<ReforgedRunePath> data;
    private String platform, version, locale;

    public com.merakianalytics.orianna.types.dto.staticdata.ReforgedRuneTree convert() {
        final com.merakianalytics.orianna.types.dto.staticdata.ReforgedRuneTree tree =
            new com.merakianalytics.orianna.types.dto.staticdata.ReforgedRuneTree(0);
        try {
            DATA.set(tree, data);
        } catch(IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("Failed to set data on ReforgedRuneTree! Report this to the orianna team!", e);
            throw new OriannaException("Failed to set data on ReforgedRuneTree! Report this to the orianna team!", e);
        }
        tree.setPlatform(platform);
        tree.setLocale(locale);
        tree.setVersion(version);
        return tree;
    }

    /**
     * @return the data
     */
    public List<ReforgedRunePath> getData() {
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
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * @param data
     *        the data to set
     */
    public void setData(final List<ReforgedRunePath> data) {
        this.data = data;
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
     * @param version
     *        the version to set
     */
    public void setVersion(final String version) {
        this.version = version;
    }
}

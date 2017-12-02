package com.merakianalytics.orianna.datastores.mongo.dto;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.Map;

import org.bson.conversions.Bson;

import com.merakianalytics.datapipelines.PipelineContext;
import com.merakianalytics.datapipelines.sources.Get;
import com.merakianalytics.orianna.datapipeline.common.Utilities;
import com.merakianalytics.orianna.types.common.Platform;

public class MongoDBDataStore extends com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore {
    public class Configuration extends com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration {}

    public MongoDBDataStore(final Configuration config) {
        super(config);
    }

    @Get(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public com.merakianalytics.orianna.types.dto.champion.Champion getChampionStatus(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number id = (Number)query.get("id");
        Utilities.checkNotNull(platform, "platform", id, "id");

        final Bson filter = and(eq("platform", platform), eq("id", id));

        return findFirstSynchronously(com.merakianalytics.orianna.types.dto.champion.Champion.class, filter);
    }
}

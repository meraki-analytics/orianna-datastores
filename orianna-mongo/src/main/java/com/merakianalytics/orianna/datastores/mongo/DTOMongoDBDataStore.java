package com.merakianalytics.orianna.datastores.mongo;

import java.util.Map;

import org.bson.conversions.Bson;

import com.google.common.collect.Lists;
import com.merakianalytics.datapipelines.PipelineContext;
import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.merakianalytics.datapipelines.sinks.Put;
import com.merakianalytics.datapipelines.sinks.PutMany;
import com.merakianalytics.datapipelines.sources.Get;
import com.merakianalytics.datapipelines.sources.GetMany;
import com.merakianalytics.orianna.datapipeline.common.Utilities;
import com.merakianalytics.orianna.types.common.Platform;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

public class DTOMongoDBDataStore extends MongoDBDataStore {
    public DTOMongoDBDataStore(final Configuration config) {
        super(config);
    }

    @Get(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public com.merakianalytics.orianna.types.dto.champion.Champion getChampion(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number id = (Number)query.get("id");
        Utilities.checkNotNull(platform, "platform", id, "id");

        final Bson filter = Filters.and(Filters.eq("platform", platform), Filters.eq("id", id));
        final MongoCollection<com.merakianalytics.orianna.types.dto.champion.Champion> collection =
            getCollection(com.merakianalytics.orianna.types.dto.champion.Champion.class);
        return collection.find(filter).first();
    }

    @SuppressWarnings("unchecked")
    @GetMany(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public CloseableIterator<com.merakianalytics.orianna.types.dto.champion.Champion> getManyChampion(final Map<String, Object> query,
        final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        Utilities.checkNotNull(platform, "platform", ids, "ids");

        final Bson filter = Filters.and(Filters.eq("platform", platform), Filters.in("id", ids));
        final MongoCollection<com.merakianalytics.orianna.types.dto.champion.Champion> collection =
            getCollection(com.merakianalytics.orianna.types.dto.champion.Champion.class);
        return new FindResult<>(collection.find(filter));
    }
    
    @Put(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public void putChampion(com.merakianalytics.orianna.types.dto.champion.Champion champion, PipelineContext context) {
        final MongoCollection<com.merakianalytics.orianna.types.dto.champion.Champion> collection =
                getCollection(com.merakianalytics.orianna.types.dto.champion.Champion.class);
        collection.insertOne(champion);
    }
    
    @PutMany(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public void putManyChampion(Iterable<com.merakianalytics.orianna.types.dto.champion.Champion> champions, PipelineContext context) {
        final MongoCollection<com.merakianalytics.orianna.types.dto.champion.Champion> collection =
                getCollection(com.merakianalytics.orianna.types.dto.champion.Champion.class);
        collection.insertMany(Lists.newArrayList(champions));
    }
}

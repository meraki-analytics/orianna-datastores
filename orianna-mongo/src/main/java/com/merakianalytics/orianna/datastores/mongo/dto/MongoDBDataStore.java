package com.merakianalytics.orianna.datastores.mongo.dto;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.compoundIndex;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.bson.BsonNumber;
import org.bson.codecs.pojo.AddUpdatedTimestamp;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.merakianalytics.datapipelines.PipelineContext;
import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.merakianalytics.datapipelines.sinks.Put;
import com.merakianalytics.datapipelines.sinks.PutMany;
import com.merakianalytics.datapipelines.sources.Get;
import com.merakianalytics.datapipelines.sources.GetMany;
import com.merakianalytics.orianna.datapipeline.common.Utilities;
import com.merakianalytics.orianna.datapipeline.common.expiration.ExpirationPeriod;
import com.merakianalytics.orianna.types.common.OriannaException;
import com.merakianalytics.orianna.types.common.Platform;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;

public class MongoDBDataStore extends com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore {
    public static class Configuration extends com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration {
        private static final Map<String, ExpirationPeriod> DEFAULT_EXPIRATION_PERIODS = ImmutableMap.<String, ExpirationPeriod> builder()
            .put(com.merakianalytics.orianna.types.dto.champion.Champion.class.getCanonicalName(), ExpirationPeriod.create(6, TimeUnit.HOURS))
            .build();

        private Map<String, ExpirationPeriod> expirationPeriods = DEFAULT_EXPIRATION_PERIODS;

        /**
         * @return the expirationPeriods
         */
        public Map<String, ExpirationPeriod> getExpirationPeriods() {
            return expirationPeriods;
        }

        /**
         * @param expirationPeriods
         *        the expirationPeriods to set
         */
        public void setExpirationPeriods(final Map<String, ExpirationPeriod> expirationPeriods) {
            this.expirationPeriods = expirationPeriods;
        }
    }

    private static Logger LOGGER = LoggerFactory.getLogger(MongoDBDataStore.class);

    public MongoDBDataStore() {
        this(new Configuration());
    }

    public MongoDBDataStore(final Configuration config) {
        super(config);
        ensureIndexes(config);
    }

    private void ensureIndexes(final Configuration config) {
        final Map<Class<?>, String[]> compositeKeys = ImmutableMap.<Class<?>, String[]> builder()
            .put(com.merakianalytics.orianna.types.dto.champion.Champion.class, new String[] {"platform", "id"}).build();

        for(final Class<?> clazz : compositeKeys.keySet()) {
            final MongoCollection<?> collection = getCollection(clazz);

            final String[] keys = compositeKeys.get(clazz);
            final Bson composite = compoundIndex(Arrays.stream(keys).map((final String key) -> ascending(key)).toArray(Bson[]::new));
            final IndexModel compositeKey = new IndexModel(composite, new IndexOptions().unique(true));

            List<IndexModel> indexes;

            final ExpirationPeriod period = config.getExpirationPeriods().get(clazz.getCanonicalName());
            if(period != null) {
                final IndexModel expiration =
                    new IndexModel(ascending(AddUpdatedTimestamp.FIELD_NAME), new IndexOptions().expireAfter(period.getPeriod(), period.getUnit()));
                indexes = Lists.newArrayList(compositeKey, expiration);
            } else {
                indexes = Lists.newArrayList(compositeKey);
            }

            final CompletableFuture<List<String>> future = new CompletableFuture<>();
            collection.createIndexes(indexes, (final List<String> results, final Throwable exception) -> {
                if(exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(results);
                }
            });

            try {
                future.get();
            } catch(InterruptedException | ExecutionException e) {
                LOGGER.error("Error creating MongoDB indexes!", e);
                throw new OriannaException("Error creating MongoDB indexes!", e);
            }
        }
    }

    @Get(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public com.merakianalytics.orianna.types.dto.champion.Champion getChampionStatus(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(com.merakianalytics.orianna.types.dto.champion.Champion.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Number id = (Number)query.get("id");
            Utilities.checkNotNull(platform, "platform", id, "id");

            return and(eq("platform", platform.getTag()), eq("id", id));
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public CloseableIterator<com.merakianalytics.orianna.types.dto.champion.Champion> getManyChampionStatus(final Map<String, Object> q,
        final PipelineContext context) {
        return find(com.merakianalytics.orianna.types.dto.champion.Champion.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<Number> iter = (Iterable<Number>)query.get("ids");
            Utilities.checkNotNull(platform, "platform", iter, "ids");

            final List<BsonNumber> ids = numbersToBson(iter);
            final Bson filter = and(eq("platform", platform.getTag()), in("id", ids));

            return FindQuery.<com.merakianalytics.orianna.types.dto.champion.Champion, BsonNumber, Number> builder().filter(filter).order(ids)
                .orderingField("id").converter(BsonNumber::longValue).index(com.merakianalytics.orianna.types.dto.champion.Champion::getId).build();
        });
    }

    @Put(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public void putChampionStatus(final com.merakianalytics.orianna.types.dto.champion.Champion champ, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.types.dto.champion.Champion.class, champ,
            (final com.merakianalytics.orianna.types.dto.champion.Champion champion) -> {
                return and(eq("platform", champion.getPlatform()), eq("id", champion.getId()));
            });
    }

    @PutMany(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public void putManyChampionStatus(final Iterable<com.merakianalytics.orianna.types.dto.champion.Champion> champions, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.types.dto.champion.Champion.class, champions,
            (final com.merakianalytics.orianna.types.dto.champion.Champion champion) -> {
                return and(eq("platform", champion.getPlatform()), eq("id", champion.getId()));
            });
    }
}

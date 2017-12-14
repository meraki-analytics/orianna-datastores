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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.codecs.pojo.AddUpdatedTimestamp;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.merakianalytics.datapipelines.PipelineContext;
import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.merakianalytics.datapipelines.iterators.CloseableIterators;
import com.merakianalytics.datapipelines.sinks.Put;
import com.merakianalytics.datapipelines.sinks.PutMany;
import com.merakianalytics.datapipelines.sources.Get;
import com.merakianalytics.datapipelines.sources.GetMany;
import com.merakianalytics.orianna.datapipeline.common.Utilities;
import com.merakianalytics.orianna.datapipeline.common.expiration.ExpirationPeriod;
import com.merakianalytics.orianna.types.common.OriannaException;
import com.merakianalytics.orianna.types.common.Platform;
import com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries;
import com.merakianalytics.orianna.types.dto.championmastery.ChampionMastery;
import com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteryScore;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;

public class MongoDBDataStore extends com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore {
    public static class Configuration extends com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration {
        private static final Map<String, ExpirationPeriod> DEFAULT_EXPIRATION_PERIODS = ImmutableMap.<String, ExpirationPeriod> builder()
            .put(com.merakianalytics.orianna.types.dto.champion.Champion.class.getCanonicalName(), ExpirationPeriod.create(6, TimeUnit.HOURS))
            .put(com.merakianalytics.orianna.types.dto.champion.ChampionList.class.getCanonicalName(), ExpirationPeriod.create(6, TimeUnit.HOURS))
            .put(ChampionMastery.class.getCanonicalName(), ExpirationPeriod.create(2, TimeUnit.HOURS))
            .put(ChampionMasteries.class.getCanonicalName(), ExpirationPeriod.create(2, TimeUnit.HOURS))
            .put(ChampionMasteryScore.class.getCanonicalName(), ExpirationPeriod.create(2, TimeUnit.HOURS))
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
            .put(com.merakianalytics.orianna.types.dto.champion.Champion.class, new String[] {"platform", "id"})
            .put(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, new String[] {"platform", "freeToPlay"})
            .put(ChampionMastery.class, new String[] {"platform", "playerId", "championId"})
            .put(ChampionMasteries.class, new String[] {"platform", "summonerId"})
            .put(ChampionMasteryScore.class, new String[] {"platform", "summonerId"})
            .build();

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

    @Get(ChampionMasteries.class)
    public ChampionMasteries getChampionMasteries(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries.class, q,
            (final Map<String, Object> query) -> {
                final Platform platform = (Platform)query.get("platform");
                final Number summonerId = (Number)query.get("summonerId");
                Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

                return and(eq("platform", platform.getTag()), eq("summonerId", summonerId));
            }).convert();
    }

    @Get(ChampionMastery.class)
    public ChampionMastery getChampionMastery(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(ChampionMastery.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Number summonerId = (Number)query.get("summonerId");
            final Number championId = (Number)query.get("championId");
            Utilities.checkNotNull(platform, "platform", summonerId, "summonerId", championId, "championId");

            return and(eq("platform", platform.getTag()), eq("playerId", summonerId), eq("championId", championId));
        });
    }

    @Get(ChampionMasteryScore.class)
    public ChampionMasteryScore getChampionMasteryScore(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(ChampionMasteryScore.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Number summonerId = (Number)query.get("summonerId");
            Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

            return and(eq("platform", platform.getTag()), eq("summonerId", summonerId));
        });
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

    @Get(com.merakianalytics.orianna.types.dto.champion.ChampionList.class)
    public com.merakianalytics.orianna.types.dto.champion.ChampionList getChampionStatusList(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Boolean freeToPlay = query.get("freeToPlay") == null ? Boolean.FALSE : (Boolean)query.get("freeToPlay");
            Utilities.checkNotNull(platform, "platform");

            return and(eq("platform", platform.getTag()), eq("freeToPlay", freeToPlay));
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMasteries.class)
    public CloseableIterator<ChampionMasteries> getManyChampionMasteries(final Map<String, Object> q, final PipelineContext context) {
        return CloseableIterators.transform(
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries.class, q, (final Map<String, Object> query) -> {
                final Platform platform = (Platform)query.get("platform");
                final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
                Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

                final List<BsonNumber> summonerIds = numbersToBson(iter);
                final Bson filter = and(eq("platform", platform), in("summonerId", summonerIds));

                return FindQuery.<com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries, BsonNumber, Long> builder()
                    .filter(filter).order(summonerIds)
                    .orderingField("summonerId").converter(BsonNumber::longValue)
                    .index(com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries::getSummonerId).build();
            }), com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries::convert);
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMastery.class)
    public CloseableIterator<ChampionMastery> getManyChampionMastery(final Map<String, Object> q, final PipelineContext context) {
        return find(ChampionMastery.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Number summonerId = (Number)query.get("summonerId");
            final Iterable<Number> iter = (Iterable<Number>)query.get("championIds");
            Utilities.checkNotNull(platform, "platform", summonerId, "summonerId", iter, "championIds");

            final List<BsonNumber> championIds = numbersToBson(iter);
            final Bson filter = and(eq("platform", platform), eq("playerId", summonerId), in("championId", championIds));

            return FindQuery.<ChampionMastery, BsonNumber, Long> builder().filter(filter).order(championIds)
                .orderingField("championId").converter(BsonNumber::longValue).index(ChampionMastery::getChampionId).build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMasteryScore.class)
    public CloseableIterator<ChampionMasteryScore> getManyChampionMasteryScore(final Map<String, Object> q, final PipelineContext context) {
        return find(ChampionMasteryScore.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
            Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

            final List<BsonNumber> summonerIds = numbersToBson(iter);
            final Bson filter = and(eq("platform", platform), in("summonerId", summonerIds));

            return FindQuery.<ChampionMasteryScore, BsonNumber, Long> builder().filter(filter).order(summonerIds)
                .orderingField("summonerId").converter(BsonNumber::longValue).index(ChampionMasteryScore::getSummonerId).build();
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

            return FindQuery.<com.merakianalytics.orianna.types.dto.champion.Champion, BsonNumber, Long> builder().filter(filter).order(ids)
                .orderingField("id").converter(BsonNumber::longValue).index(com.merakianalytics.orianna.types.dto.champion.Champion::getId).build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(com.merakianalytics.orianna.types.dto.champion.ChampionList.class)
    public CloseableIterator<com.merakianalytics.orianna.types.dto.champion.ChampionList> getManyChampionStatusList(final Map<String, Object> q,
        final PipelineContext context) {
        return find(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, q, (final Map<String, Object> query) -> {
            final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
            final Boolean freeToPlay = query.get("freeToPlay") == null ? Boolean.FALSE : (Boolean)query.get("freeToPlay");
            Utilities.checkNotNull(iter, "platforms");

            final List<BsonString> platforms = StreamSupport.stream(iter.spliterator(), false)
                .map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
            final Bson filter = and(eq("freeToPlay", freeToPlay), in("platform", platforms));

            return FindQuery.<com.merakianalytics.orianna.types.dto.champion.ChampionList, BsonString, String> builder().filter(filter).order(platforms)
                .orderingField("id").converter(BsonString::getValue).index(com.merakianalytics.orianna.types.dto.champion.ChampionList::getPlatform).build();
        });
    }

    @Put(ChampionMasteries.class)
    public void putChampionMasteries(final ChampionMasteries m, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries.convert(m),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries masteries) -> {
                return and(eq("platform", masteries.getPlatform()), eq("summonerId", masteries.getSummonerId()));
            });
        putManyChampionMastery(m, context);
    }

    @Put(ChampionMastery.class)
    public void putChampionMastery(final ChampionMastery m, final PipelineContext context) {
        upsert(ChampionMastery.class, m, (final ChampionMastery mastery) -> {
            return and(eq("platform", mastery.getPlatform()), eq("playerId", mastery.getPlayerId()), eq("championId", mastery.getChampionId()));
        });
    }

    @Put(ChampionMasteryScore.class)
    public void putChampionMasteryScore(final ChampionMasteryScore m, final PipelineContext context) {
        upsert(ChampionMasteryScore.class, m, (final ChampionMasteryScore masteries) -> {
            return and(eq("platform", masteries.getPlatform()), eq("summonerId", masteries.getSummonerId()));
        });
    }

    @Put(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public void putChampionStatus(final com.merakianalytics.orianna.types.dto.champion.Champion champ, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.types.dto.champion.Champion.class, champ,
            (final com.merakianalytics.orianna.types.dto.champion.Champion champion) -> {
                return and(eq("platform", champion.getPlatform()), eq("id", champion.getId()));
            });
    }

    @Put(com.merakianalytics.orianna.types.dto.champion.ChampionList.class)
    public void putChampionStatusList(final com.merakianalytics.orianna.types.dto.champion.ChampionList champs, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, champs,
            (final com.merakianalytics.orianna.types.dto.champion.ChampionList champions) -> {
                return and(eq("platform", champions.getPlatform()), eq("freeToPlay", champions.isFreeToPlay()));
            });
        putManyChampionStatus(champs.getChampions(), context);
    }

    @PutMany(ChampionMasteries.class)
    public void putManyChampionMasteries(final Iterable<ChampionMasteries> m, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries.class,
            Iterables.transform(m, com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries masteries) -> {
                return and(eq("platform", masteries.getPlatform()), eq("summonerId", masteries.getSummonerId()));
            });
        putManyChampionMastery(Iterables.concat(m), context);
    }

    @PutMany(ChampionMastery.class)
    public void putManyChampionMastery(final Iterable<ChampionMastery> m, final PipelineContext context) {
        upsert(ChampionMastery.class, m, (final ChampionMastery mastery) -> {
            return and(eq("platform", mastery.getPlatform()), eq("playerId", mastery.getPlayerId()), eq("championId", mastery.getChampionId()));
        });
    }

    @PutMany(ChampionMasteryScore.class)
    public void putManyChampionMasteryScore(final Iterable<ChampionMasteryScore> m, final PipelineContext context) {
        upsert(ChampionMasteryScore.class, m, (final ChampionMasteryScore masteries) -> {
            return and(eq("platform", masteries.getPlatform()), eq("summonerId", masteries.getSummonerId()));
        });
    }

    @PutMany(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public void putManyChampionStatus(final Iterable<com.merakianalytics.orianna.types.dto.champion.Champion> champions, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.types.dto.champion.Champion.class, champions,
            (final com.merakianalytics.orianna.types.dto.champion.Champion champion) -> {
                return and(eq("platform", champion.getPlatform()), eq("id", champion.getId()));
            });
    }

    @PutMany(com.merakianalytics.orianna.types.dto.champion.ChampionList.class)
    public void putManyChampionStatusList(final Iterable<com.merakianalytics.orianna.types.dto.champion.ChampionList> champs, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, champs,
            (final com.merakianalytics.orianna.types.dto.champion.ChampionList champions) -> {
                return and(eq("platform", champions.getPlatform()), eq("freeToPlay", champions.isFreeToPlay()));
            });
        putManyChampionStatus(Iterables.concat(StreamSupport.stream(champs.spliterator(), false)
            .map(com.merakianalytics.orianna.types.dto.champion.ChampionList::getChampions).collect(Collectors.toList())), context);
    }
}

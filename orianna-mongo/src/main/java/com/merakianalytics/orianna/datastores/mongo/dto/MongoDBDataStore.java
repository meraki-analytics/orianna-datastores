package com.merakianalytics.orianna.datastores.mongo.dto;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.compoundIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import com.google.common.collect.ImmutableSet;
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
import com.merakianalytics.orianna.types.common.Queue;
import com.merakianalytics.orianna.types.common.Tier;
import com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries;
import com.merakianalytics.orianna.types.dto.championmastery.ChampionMastery;
import com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteryScore;
import com.merakianalytics.orianna.types.dto.league.LeagueList;
import com.merakianalytics.orianna.types.dto.league.SummonerPositions;
import com.merakianalytics.orianna.types.dto.match.Match;
import com.merakianalytics.orianna.types.dto.match.MatchTimeline;
import com.merakianalytics.orianna.types.dto.match.TournamentMatches;
import com.merakianalytics.orianna.types.dto.spectator.CurrentGameInfo;
import com.merakianalytics.orianna.types.dto.spectator.FeaturedGames;
import com.merakianalytics.orianna.types.dto.staticdata.Champion;
import com.merakianalytics.orianna.types.dto.staticdata.ChampionList;
import com.merakianalytics.orianna.types.dto.staticdata.Item;
import com.merakianalytics.orianna.types.dto.staticdata.ItemList;
import com.merakianalytics.orianna.types.dto.staticdata.LanguageStrings;
import com.merakianalytics.orianna.types.dto.staticdata.Languages;
import com.merakianalytics.orianna.types.dto.staticdata.MapData;
import com.merakianalytics.orianna.types.dto.staticdata.Mastery;
import com.merakianalytics.orianna.types.dto.staticdata.MasteryList;
import com.merakianalytics.orianna.types.dto.staticdata.ProfileIconData;
import com.merakianalytics.orianna.types.dto.staticdata.Realm;
import com.merakianalytics.orianna.types.dto.staticdata.Rune;
import com.merakianalytics.orianna.types.dto.staticdata.RuneList;
import com.merakianalytics.orianna.types.dto.staticdata.SummonerSpell;
import com.merakianalytics.orianna.types.dto.staticdata.SummonerSpellList;
import com.merakianalytics.orianna.types.dto.staticdata.Versions;
import com.merakianalytics.orianna.types.dto.status.ShardStatus;
import com.merakianalytics.orianna.types.dto.summoner.Summoner;
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
            .put(LeagueList.class.getCanonicalName(), ExpirationPeriod.create(45, TimeUnit.MINUTES))
            .put(SummonerPositions.class.getCanonicalName(), ExpirationPeriod.create(2, TimeUnit.HOURS))
            .put(CurrentGameInfo.class.getCanonicalName(), ExpirationPeriod.create(5, TimeUnit.MINUTES))
            .put(FeaturedGames.class.getCanonicalName(), ExpirationPeriod.create(5, TimeUnit.MINUTES))
            .put(Realm.class.getCanonicalName(), ExpirationPeriod.create(6, TimeUnit.HOURS))
            .put(Versions.class.getCanonicalName(), ExpirationPeriod.create(6, TimeUnit.HOURS))
            .put(ShardStatus.class.getCanonicalName(), ExpirationPeriod.create(15, TimeUnit.MINUTES))
            .put(Summoner.class.getCanonicalName(), ExpirationPeriod.create(12, TimeUnit.HOURS))
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

    private static final Set<Tier> LEAGUE_LIST_ENDPOINTS = ImmutableSet.of(Tier.MASTER, Tier.CHALLENGER);
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBDataStore.class);

    private static String getCurrentVersion(final Platform platform, final PipelineContext context) {
        final Realm realm = context.getPipeline().get(Realm.class, ImmutableMap.<String, Object> of("platform", platform));
        return realm.getV();
    }

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
            .put(LeagueList.class, new String[] {"platform", "leagueId"})
            .put(SummonerPositions.class, new String[] {"platform", "summonerId"})
            .put(Match.class, new String[] {"platform", "gameId"})
            // TODO: Matchlist
            .put(MatchTimeline.class, new String[] {"platform", "matchId"})
            .put(TournamentMatches.class, new String[] {"platform", "tournamentCode"})
            .put(CurrentGameInfo.class, new String[] {"platformId", "summonerId"})
            .put(FeaturedGames.class, new String[] {"platform"})
            .put(Champion.class, new String[] {"platform", "id", "version", "locale", "includedData"})
            .put(ChampionList.class, new String[] {"platform", "version", "locale", "includedData", "dataById"})
            .put(Item.class, new String[] {"platform", "id", "version", "locale", "includedData"})
            .put(ItemList.class, new String[] {"platform", "version", "locale", "includedData"})
            .put(Languages.class, new String[] {"platform"})
            .put(LanguageStrings.class, new String[] {"platform", "version", "locale"})
            .put(MapData.class, new String[] {"platform", "version", "locale"})
            .put(Mastery.class, new String[] {"platform", "id", "version", "locale", "includedData"})
            .put(MasteryList.class, new String[] {"platform", "version", "locale", "includedData"})
            .put(ProfileIconData.class, new String[] {"platform", "version", "locale"})
            .put(Realm.class, new String[] {"platform"})
            .put(Rune.class, new String[] {"platform", "id", "version", "locale", "includedData"})
            .put(RuneList.class, new String[] {"platform", "version", "locale", "includedData"})
            .put(SummonerSpell.class, new String[] {"platform", "id", "version", "locale", "includedData"})
            .put(SummonerSpellList.class, new String[] {"platform", "version", "locale", "includedData"})
            .put(Versions.class, new String[] {"platform"})
            .put(ShardStatus.class, new String[] {"platform"})
            .put(Summoner.class, new String[] {"platform", "id"})
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

    @SuppressWarnings("unchecked")
    @Get(Champion.class)
    public Champion getChampion(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(Champion.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Number id = (Number)query.get("id");
            final String name = (String)query.get("name");
            final String key = (String)query.get("key");
            Utilities.checkAtLeastOneNotNull(id, "id", name, "name", key, "key");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            if(id != null) {
                return and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            } else if(name != null) {
                return and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            } else {
                return and(eq("platform", platform.getTag()), eq("key", key), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Get(ChampionList.class)
    public ChampionList getChampionList(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(ChampionList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");
            final Boolean dataById = query.get("dataById") == null ? Boolean.FALSE : (Boolean)query.get("dataById");

            return and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale), eq("includedData", includedData),
                eq("dataById", dataById));
        });
    }

    @Get(ChampionMasteries.class)
    public ChampionMasteries getChampionMasteries(final Map<String, Object> q, final PipelineContext context) {
        return Optional.ofNullable(findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries.class, q,
            (final Map<String, Object> query) -> {
                final Platform platform = (Platform)query.get("platform");
                final Number summonerId = (Number)query.get("summonerId");
                Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

                return and(eq("platform", platform.getTag()), eq("summonerId", summonerId));
            })).map(com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries::convert).orElse(null);
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

    @Get(CurrentGameInfo.class)
    public CurrentGameInfo getCurrentGameInfo(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(CurrentGameInfo.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Number summonerId = (Number)query.get("summonerId");
            Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

            return and(eq("platformId", platform.getTag()), eq("summonerId", summonerId));
        });
    }

    @Get(FeaturedGames.class)
    public FeaturedGames getFeaturedGames(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(FeaturedGames.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");

            return eq("platform", platform.getTag());
        });
    }

    @SuppressWarnings("unchecked")
    @Get(Item.class)
    public Item getItem(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(Item.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Number id = (Number)query.get("id");
            final String name = (String)query.get("name");
            Utilities.checkAtLeastOneNotNull(id, "id", name, "name");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            if(id != null) {
                return and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            } else {
                return and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Get(ItemList.class)
    public ItemList getItemList(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(ItemList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            return and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        });
    }

    @Get(Languages.class)
    public Languages getLanguages(final Map<String, Object> q, final PipelineContext context) {
        return Optional.ofNullable(findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages.class, q,
            (final Map<String, Object> query) -> {
                final Platform platform = (Platform)query.get("platform");
                Utilities.checkNotNull(platform, "platform");

                return eq("platform", platform.getTag());
            })).map(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages::convert).orElse(null);
    }

    @Get(LanguageStrings.class)
    public LanguageStrings getLanguageStrings(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(LanguageStrings.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

            return and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale));
        });
    }

    @Get(LeagueList.class)
    public LeagueList getLeagueList(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(LeagueList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Tier tier = (Tier)query.get("tier");
            final Queue queue = (Queue)query.get("queue");
            final String leagueId = (String)query.get("leagueId");

            if(leagueId == null) {
                if(tier == null || queue == null) {
                    throw new IllegalArgumentException("Query was missing required parameters! Either leagueId or tier and queue must be included!");
                } else if(!LEAGUE_LIST_ENDPOINTS.contains(tier) || !Queue.RANKED.contains(queue)) {
                    return null;
                }
            }

            if(leagueId != null) {
                return and(eq("platform", platform.getTag()), eq("leagueId", leagueId));
            } else {
                return and(eq("platform", platform.getTag()), eq("tier", tier.name()), eq("queue", queue.name()));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Champion.class)
    public CloseableIterator<Champion> getManyChampion(final Map<String, Object> q, final PipelineContext context) {
        return find(Champion.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
            final Iterable<String> names = (Iterable<String>)query.get("names");
            final Iterable<String> keys = (Iterable<String>)query.get("keys");
            Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names", keys, "keys");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            if(ids != null) {
                final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("id").build();
            } else if(names != null) {
                final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("name").build();
            } else {
                final List<BsonString> order = StreamSupport.stream(keys.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("key", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("key").build();
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionList.class)
    public CloseableIterator<ChampionList> getManyChampionList(final Map<String, Object> q, final PipelineContext context) {
        return find(ChampionList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<String> iter = (Iterable<String>)query.get("versions");
            Utilities.checkNotNull(platform, "platform", iter, "versions");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");
            final Boolean dataById = query.get("dataById") == null ? Boolean.FALSE : (Boolean)query.get("dataById");

            final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform), in("version", versions), eq("locale", locale), eq("includedData", includedData), eq("dataById", dataById));

            return FindQuery.builder().filter(filter).order(versions).orderingField("version").build();
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

                return FindQuery.builder().filter(filter).order(summonerIds).orderingField("summonerId").build();
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

            return FindQuery.builder().filter(filter).order(championIds).orderingField("championId").build();
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

            return FindQuery.builder().filter(filter).order(summonerIds).orderingField("summonerId").build();
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

            return FindQuery.builder().filter(filter).order(ids).orderingField("id").build();
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

            return FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(CurrentGameInfo.class)
    public CloseableIterator<CurrentGameInfo> getManyCurrentGameInfo(final Map<String, Object> q, final PipelineContext context) {
        return find(CurrentGameInfo.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
            Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

            final List<BsonNumber> summonerIds = numbersToBson(iter);
            final Bson filter = and(eq("platformId", platform), in("summonerId", summonerIds));

            return FindQuery.builder().filter(filter).order(summonerIds).orderingField("summonerId").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(FeaturedGames.class)
    public CloseableIterator<FeaturedGames> getManyFeaturedGames(final Map<String, Object> q, final PipelineContext context) {
        return find(FeaturedGames.class, q, (final Map<String, Object> query) -> {
            final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
            Utilities.checkNotNull(iter, "platforms");

            final List<BsonString> platforms = StreamSupport.stream(iter.spliterator(), false)
                .map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
            final Bson filter = in("platform", platforms);

            return FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Item.class)
    public CloseableIterator<Item> getManyItem(final Map<String, Object> q, final PipelineContext context) {
        return find(Item.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
            final Iterable<String> names = (Iterable<String>)query.get("names");
            Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            if(ids != null) {
                final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("id").build();
            } else {
                final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("name").build();
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(ItemList.class)
    public CloseableIterator<ItemList> getManyItemList(final Map<String, Object> q, final PipelineContext context) {
        return find(ItemList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<String> iter = (Iterable<String>)query.get("versions");
            Utilities.checkNotNull(platform, "platform", iter, "versions");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform), in("version", versions), eq("locale", locale), eq("includedData", includedData));

            return FindQuery.builder().filter(filter).order(versions).orderingField("version").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Languages.class)
    public CloseableIterator<Languages> getManyLanguages(final Map<String, Object> q, final PipelineContext context) {
        return CloseableIterators.transform(
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages.class, q, (final Map<String, Object> query) -> {
                final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
                Utilities.checkNotNull(iter, "platforms");

                final List<BsonString> platforms = StreamSupport.stream(iter.spliterator(), false)
                    .map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
                final Bson filter = in("platform", platforms);

                return FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();
            }), com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages::convert);
    }

    @SuppressWarnings("unchecked")
    @GetMany(LanguageStrings.class)
    public CloseableIterator<LanguageStrings> getManyLanguageStrings(final Map<String, Object> q, final PipelineContext context) {
        return find(LanguageStrings.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<String> iter = (Iterable<String>)query.get("locales");
            Utilities.checkNotNull(platform, "platform", iter, "locales");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");

            final List<BsonString> locales = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform), in("locale", locales), eq("version", version));

            return FindQuery.builder().filter(filter).order(locales).orderingField("locale").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(LeagueList.class)
    public CloseableIterator<LeagueList> getManyLeagueList(final Map<String, Object> q, final PipelineContext context) {
        return find(LeagueList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Tier tier = (Tier)query.get("tier");
            final Iterable<Queue> queues = (Iterable<Queue>)query.get("queues");
            final Iterable<String> leagueIds = (Iterable<String>)query.get("leagueIds");

            if(leagueIds == null) {
                if(tier == null || queues == null) {
                    throw new IllegalArgumentException("Query was missing required parameters! Either leagueIds or tier and queues must be included!");
                } else if(!LEAGUE_LIST_ENDPOINTS.contains(tier)) {
                    return null;
                }
            }

            if(leagueIds != null) {
                final List<BsonString> ids = StreamSupport.stream(leagueIds.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
                final Bson filter = and(eq("platform", platform), in("leagueId", ids));
                return FindQuery.builder().filter(filter).order(ids).orderingField("leagueId").build();
            } else {
                final List<BsonString> ids =
                    StreamSupport.stream(queues.spliterator(), false).map((final Queue queue) -> new BsonString(queue.name())).collect(Collectors.toList());
                final Bson filter = and(eq("platform", platform), eq("tier", tier.name()), in("queue", ids));
                return FindQuery.builder().filter(filter).order(ids).orderingField("queue").build();
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(MapData.class)
    public CloseableIterator<MapData> getManyMapData(final Map<String, Object> q, final PipelineContext context) {
        return find(MapData.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<String> iter = (Iterable<String>)query.get("versions");
            Utilities.checkNotNull(platform, "platform", iter, "versions");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

            final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform), in("version", versions), eq("locale", locale));

            return FindQuery.builder().filter(filter).order(versions).orderingField("version").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Mastery.class)
    public CloseableIterator<Mastery> getManyMastery(final Map<String, Object> q, final PipelineContext context) {
        return find(Mastery.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
            final Iterable<String> names = (Iterable<String>)query.get("names");
            Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            if(ids != null) {
                final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("id").build();
            } else {
                final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("name").build();
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(MasteryList.class)
    public CloseableIterator<MasteryList> getManyMasteryList(final Map<String, Object> q, final PipelineContext context) {
        return find(MasteryList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<String> iter = (Iterable<String>)query.get("versions");
            Utilities.checkNotNull(platform, "platform", iter, "versions");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform), in("version", versions), eq("locale", locale), eq("includedData", includedData));

            return FindQuery.builder().filter(filter).order(versions).orderingField("version").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Match.class)
    public CloseableIterator<Match> getManyMatch(final Map<String, Object> q, final PipelineContext context) {
        return find(Match.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<Number> iter = (Iterable<Number>)query.get("matchIds");
            final String tournamentCode = (String)query.get("tournamentCode");
            Utilities.checkNotNull(platform, "platform", iter, "matchIds");

            final List<BsonNumber> matchIds = numbersToBson(iter);

            final Bson filter;
            if(tournamentCode == null) {
                filter = and(eq("platformId", platform), in("gameId", matchIds));
            } else {
                filter = and(eq("platformId", platform), eq("tournamentCode", tournamentCode), in("gameId", matchIds));
            }

            return FindQuery.builder().filter(filter).order(matchIds).orderingField("gameId").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(MatchTimeline.class)
    public CloseableIterator<MatchTimeline> getManyMatchTimeline(final Map<String, Object> q, final PipelineContext context) {
        return find(MatchTimeline.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<Number> iter = (Iterable<Number>)query.get("matchIds");
            Utilities.checkNotNull(platform, "platform", iter, "matchIds");

            final List<BsonNumber> matchIds = numbersToBson(iter);
            final Bson filter = and(eq("platform", platform), in("matchId", matchIds));

            return FindQuery.builder().filter(filter).order(matchIds).orderingField("matchId").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(ProfileIconData.class)
    public CloseableIterator<ProfileIconData> getManyProfileIconData(final Map<String, Object> q, final PipelineContext context) {
        return find(ProfileIconData.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<String> iter = (Iterable<String>)query.get("versions");
            Utilities.checkNotNull(platform, "platform", iter, "versions");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

            final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform), in("version", versions), eq("locale", locale));

            return FindQuery.builder().filter(filter).order(versions).orderingField("version").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Realm.class)
    public CloseableIterator<Realm> getManyRealm(final Map<String, Object> q, final PipelineContext context) {
        return find(Realm.class, q, (final Map<String, Object> query) -> {
            final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
            Utilities.checkNotNull(iter, "platforms");

            final List<BsonString> platforms = StreamSupport.stream(iter.spliterator(), false)
                .map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
            final Bson filter = in("platform", platforms);

            return FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Rune.class)
    public CloseableIterator<Rune> getManyRune(final Map<String, Object> q, final PipelineContext context) {
        return find(Rune.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
            final Iterable<String> names = (Iterable<String>)query.get("names");
            Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            if(ids != null) {
                final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("id").build();
            } else {
                final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("name").build();
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(RuneList.class)
    public CloseableIterator<RuneList> getManyRuneList(final Map<String, Object> q, final PipelineContext context) {
        return find(RuneList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<String> iter = (Iterable<String>)query.get("versions");
            Utilities.checkNotNull(platform, "platform", iter, "versions");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform), in("version", versions), eq("locale", locale), eq("includedData", includedData));

            return FindQuery.builder().filter(filter).order(versions).orderingField("version").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(ShardStatus.class)
    public CloseableIterator<ShardStatus> getManyShardStatus(final Map<String, Object> q, final PipelineContext context) {
        return find(ShardStatus.class, q, (final Map<String, Object> query) -> {
            final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
            Utilities.checkNotNull(iter, "platforms");

            final List<BsonString> platforms = StreamSupport.stream(iter.spliterator(), false)
                .map((final Platform platform) -> new BsonString(platform.getTag().toLowerCase())).collect(Collectors.toList());
            final Bson filter = in("region_tag", platforms);

            return FindQuery.builder().filter(filter).order(platforms).orderingField("region_tag").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Summoner.class)
    public CloseableIterator<Summoner> getManySummoner(final Map<String, Object> q, final PipelineContext context) {
        return find(Summoner.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Iterable<Number> summonerIds = (Iterable<Number>)query.get("ids");
            final Iterable<Number> accountIds = (Iterable<Number>)query.get("accountIds");
            final Iterable<String> summonerNames = (Iterable<String>)query.get("names");
            Utilities.checkAtLeastOneNotNull(summonerIds, "ids", accountIds, "accountIds", summonerNames, "names");

            if(summonerIds != null) {
                final List<BsonNumber> order =
                    StreamSupport.stream(summonerIds.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("id", order));
                return FindQuery.builder().filter(filter).order(order).orderingField("id").build();
            } else if(summonerNames != null) {
                final List<BsonString> order = StreamSupport.stream(summonerNames.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("name", order));
                return FindQuery.builder().filter(filter).order(order).orderingField("name").build();
            } else {
                final List<BsonNumber> order = StreamSupport.stream(accountIds.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("accountId", order));
                return FindQuery.builder().filter(filter).order(order).orderingField("accountId").build();
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerPositions.class)
    public CloseableIterator<SummonerPositions> getManySummonerPositions(final Map<String, Object> q, final PipelineContext context) {
        return CloseableIterators
            .transform(find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions.class, q, (final Map<String, Object> query) -> {
                final Platform platform = (Platform)query.get("platform");
                final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
                Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

                final List<BsonNumber> summonerIds = numbersToBson(iter);
                final Bson filter = and(eq("platform", platform), in("summonerId", summonerIds));

                return FindQuery.builder().filter(filter).order(summonerIds).orderingField("summonerId").build();
            }), com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions::convert);
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerSpell.class)
    public CloseableIterator<SummonerSpell> getManySummonerSpell(final Map<String, Object> q, final PipelineContext context) {
        return find(SummonerSpell.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
            final Iterable<String> names = (Iterable<String>)query.get("names");
            Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            if(ids != null) {
                final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("id").build();
            } else {
                final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
                final Bson filter =
                    and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
                return FindQuery.builder().filter(filter).order(order).orderingField("name").build();
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerSpellList.class)
    public CloseableIterator<SummonerSpellList> getManySummonerSpellList(final Map<String, Object> q, final PipelineContext context) {
        return find(SummonerSpellList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Iterable<String> iter = (Iterable<String>)query.get("versions");
            Utilities.checkNotNull(platform, "platform", iter, "versions");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform), in("version", versions), eq("locale", locale), eq("includedData", includedData));

            return FindQuery.builder().filter(filter).order(versions).orderingField("version").build();
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(TournamentMatches.class)
    public CloseableIterator<TournamentMatches> getManyTournamentMatches(final Map<String, Object> q, final PipelineContext context) {
        return CloseableIterators.transform(
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches.class, q, (final Map<String, Object> query) -> {
                final Platform platform = (Platform)query.get("platform");
                final Iterable<String> iter = (Iterable<String>)query.get("tournamentCodes");
                Utilities.checkNotNull(platform, "platform", iter, "tournamentCodes");

                final List<BsonString> tournamentCodes = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
                final Bson filter = and(eq("platform", platform), in("tournamentCode", tournamentCodes));

                return FindQuery.builder().filter(filter).order(tournamentCodes).orderingField("tournamentCode").build();
            }), com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches::convert);
    }

    @SuppressWarnings("unchecked")
    @GetMany(Versions.class)
    public CloseableIterator<Versions> getManyVersions(final Map<String, Object> q, final PipelineContext context) {
        return CloseableIterators.transform(
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions.class, q, (final Map<String, Object> query) -> {
                final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
                Utilities.checkNotNull(iter, "platforms");

                final List<BsonString> platforms = StreamSupport.stream(iter.spliterator(), false)
                    .map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
                final Bson filter = in("platform", platforms);

                return FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();
            }), com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions::convert);
    }

    @Get(MapData.class)
    public MapData getMapData(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(MapData.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

            return and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale));
        });
    }

    @SuppressWarnings("unchecked")
    @Get(Mastery.class)
    public Mastery getMastery(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(Mastery.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Number id = (Number)query.get("id");
            final String name = (String)query.get("name");
            Utilities.checkAtLeastOneNotNull(id, "id", name, "name");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            if(id != null) {
                return and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            } else {
                return and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Get(MasteryList.class)
    public MasteryList getMasteryList(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(MasteryList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            return and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        });
    }

    @Get(Match.class)
    public Match getMatch(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(Match.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Number matchId = (Number)query.get("matchId");
            final String tournamentCode = (String)query.get("tournamentCode");
            Utilities.checkNotNull(platform, "platform", matchId, "matchId");

            if(tournamentCode == null) {
                return and(eq("platformId", platform.getTag()), eq("gameId", matchId));
            } else {
                return and(eq("platformId", platform.getTag()), eq("gameId", matchId), eq("tournamentCode", tournamentCode));
            }
        });
    }

    @Get(MatchTimeline.class)
    public MatchTimeline getMatchTimeline(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(MatchTimeline.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            final Number matchId = (Number)query.get("matchId");
            Utilities.checkNotNull(platform, "platform", matchId, "matchId");

            return and(eq("platform", platform.getTag()), eq("matchId", matchId));
        });
    }

    @Get(ProfileIconData.class)
    public ProfileIconData getProfileIconData(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(ProfileIconData.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

            return and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale));
        });
    }

    @Get(Realm.class)
    public Realm getRealm(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(Realm.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");

            return eq("platform", platform.getTag());
        });
    }

    @SuppressWarnings("unchecked")
    @Get(Rune.class)
    public Rune getRune(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(Rune.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Number id = (Number)query.get("id");
            final String name = (String)query.get("name");
            Utilities.checkAtLeastOneNotNull(id, "id", name, "name");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            if(id != null) {
                return and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            } else {
                return and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Get(RuneList.class)
    public RuneList getRuneList(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(RuneList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            return and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        });
    }

    @Get(ShardStatus.class)
    public ShardStatus getShardStatus(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(ShardStatus.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");

            return eq("region_tag", platform.getTag().toLowerCase());
        });
    }

    @Get(Summoner.class)
    public Summoner getSummoner(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(Summoner.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Number summonerId = (Number)query.get("id");
            final Number accountId = (Number)query.get("accountId");
            final String summonerName = (String)query.get("name");
            Utilities.checkAtLeastOneNotNull(summonerId, "id", accountId, "accountId", summonerName, "name");

            if(summonerId != null) {
                return and(eq("platform", platform.getTag()), eq("id", summonerId));
            } else if(summonerName != null) {
                return and(eq("platform", platform.getTag()), eq("name", summonerName));
            } else {
                return and(eq("platform", platform.getTag()), eq("accountId", accountId));
            }
        });
    }

    @Get(SummonerPositions.class)
    public SummonerPositions getSummonerPositions(final Map<String, Object> q, final PipelineContext context) {
        return Optional.ofNullable(
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions.class, q, (final Map<String, Object> query) -> {
                final Platform platform = (Platform)query.get("platform");
                final Number summonerId = (Number)query.get("summonerId");
                Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

                return and(eq("platform", platform.getTag()), eq("summonerId", summonerId));
            })).map(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions::convert).orElse(null);
    }

    @SuppressWarnings("unchecked")
    @Get(SummonerSpell.class)
    public SummonerSpell getSummonerSpell(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(SummonerSpell.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final Number id = (Number)query.get("id");
            final String name = (String)query.get("name");
            Utilities.checkAtLeastOneNotNull(id, "id", name, "name");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            if(id != null) {
                return and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            } else {
                return and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Get(SummonerSpellList.class)
    public SummonerSpellList getSummonerSpellList(final Map<String, Object> q, final PipelineContext context) {
        return findFirst(SummonerSpellList.class, q, (final Map<String, Object> query) -> {
            final Platform platform = (Platform)query.get("platform");
            Utilities.checkNotNull(platform, "platform");
            final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
            final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
            final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

            return and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        });
    }

    @Get(TournamentMatches.class)
    public TournamentMatches getTournamentMatches(final Map<String, Object> q, final PipelineContext context) {
        return Optional.ofNullable(findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches.class, q,
            (final Map<String, Object> query) -> {
                final Platform platform = (Platform)query.get("platform");
                final String tournamentCode = (String)query.get("tournamentCode");
                Utilities.checkNotNull(platform, "platform", tournamentCode, "tournamentCode");

                return and(eq("platform", platform.getTag()), eq("tournamentCode", tournamentCode));
            })).map(com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches::convert).orElse(null);
    }

    @Get(Versions.class)
    public Versions getVersions(final Map<String, Object> q, final PipelineContext context) {
        return Optional.ofNullable(findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions.class, q,
            (final Map<String, Object> query) -> {
                final Platform platform = (Platform)query.get("platform");
                Utilities.checkNotNull(platform, "platform");

                return eq("platform", platform.getTag());
            })).map(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions::convert).orElse(null);
    }

    @Put(Champion.class)
    public void putChampion(final Champion c, final PipelineContext context) {
        upsert(Champion.class, c, (final Champion champion) -> {
            return and(eq("platform", champion.getPlatform()), eq("id", champion.getId()), eq("version", champion.getVersion()),
                eq("locale", champion.getLocale()), eq("includedData", champion.getIncludedData()));
        });
    }

    @Put(ChampionList.class)
    public void putChampionList(final ChampionList l, final PipelineContext context) {
        upsert(ChampionList.class, l, (final ChampionList list) -> {
            return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                eq("includedData", list.getIncludedData()), eq("dataById", list.isDataById()));
        });
        putManyChampion(l.getData().values(), context);
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

    @Put(CurrentGameInfo.class)
    public void putCurrentGameInfo(final CurrentGameInfo i, final PipelineContext context) {
        upsert(CurrentGameInfo.class, i, (final CurrentGameInfo info) -> {
            return and(eq("platformId", info.getPlatformId()), eq("summonerId", info.getSummonerId()));
        });
    }

    @Put(FeaturedGames.class)
    public void putFeaturedGames(final FeaturedGames g, final PipelineContext context) {
        upsert(FeaturedGames.class, g, (final FeaturedGames games) -> {
            return eq("platform", games.getPlatform());
        });
    }

    @Put(Item.class)
    public void putItem(final Item i, final PipelineContext context) {
        upsert(Item.class, i, (final Item item) -> {
            return and(eq("platform", item.getPlatform()), eq("id", item.getId()), eq("version", item.getVersion()),
                eq("locale", item.getLocale()), eq("includedData", item.getIncludedData()));
        });
    }

    @Put(ItemList.class)
    public void putItemList(final ItemList l, final PipelineContext context) {
        upsert(ItemList.class, l, (final ItemList list) -> {
            return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                eq("includedData", list.getIncludedData()));
        });
        putManyItem(l.getData().values(), context);
    }

    @Put(Languages.class)
    public void putLanguages(final Languages l, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages.convert(l),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages languages) -> {
                return eq("platform", languages.getPlatform());
            });
    }

    @Put(LanguageStrings.class)
    public void putLanguageStrings(final LanguageStrings s, final PipelineContext context) {
        upsert(LanguageStrings.class, s, (final LanguageStrings strings) -> {
            return and(eq("platform", strings.getPlatform()), eq("locale", strings.getLocale()), eq("version", strings.getVersion()));
        });
    }

    @Put(LeagueList.class)
    public void putLeagueList(final LeagueList l, final PipelineContext context) {
        upsert(LeagueList.class, l, (final LeagueList list) -> {
            return and(eq("platform", list.getPlatform()), eq("leagueId", list.getLeagueId()));
        });
    }

    @PutMany(Champion.class)
    public void putManyChampion(final Iterable<Champion> c, final PipelineContext context) {
        upsert(Champion.class, c, (final Champion champion) -> {
            return and(eq("platform", champion.getPlatform()), eq("id", champion.getId()), eq("version", champion.getVersion()),
                eq("locale", champion.getLocale()), eq("includedData", champion.getIncludedData()));
        });
    }

    @PutMany(ChampionList.class)
    public void putManyChampionList(final Iterable<ChampionList> l, final PipelineContext context) {
        upsert(ChampionList.class, l, (final ChampionList list) -> {
            return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                eq("includedData", list.getIncludedData()), eq("dataById", list.isDataById()));
        });
        putManyChampion(
            Iterables.concat(
                StreamSupport.stream(l.spliterator(), false).map((final ChampionList champions) -> champions.getData().values()).collect(Collectors.toList())),
            context);
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

    @PutMany(CurrentGameInfo.class)
    public void putManyCurrentGameInfo(final Iterable<CurrentGameInfo> i, final PipelineContext context) {
        upsert(CurrentGameInfo.class, i, (final CurrentGameInfo info) -> {
            return and(eq("platformId", info.getPlatformId()), eq("summonerId", info.getSummonerId()));
        });
    }

    @PutMany(FeaturedGames.class)
    public void putManyFeaturedGames(final Iterable<FeaturedGames> g, final PipelineContext context) {
        upsert(FeaturedGames.class, g, (final FeaturedGames games) -> {
            return eq("platform", games.getPlatform());
        });
    }

    @PutMany(Item.class)
    public void putManyItem(final Iterable<Item> i, final PipelineContext context) {
        upsert(Item.class, i, (final Item item) -> {
            return and(eq("platform", item.getPlatform()), eq("id", item.getId()), eq("version", item.getVersion()),
                eq("locale", item.getLocale()), eq("includedData", item.getIncludedData()));
        });
    }

    @PutMany(ItemList.class)
    public void putManyItemList(final Iterable<ItemList> l, final PipelineContext context) {
        upsert(ItemList.class, l, (final ItemList list) -> {
            return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                eq("includedData", list.getIncludedData()));
        });
        putManyItem(
            Iterables.concat(
                StreamSupport.stream(l.spliterator(), false).map((final ItemList items) -> items.getData().values()).collect(Collectors.toList())),
            context);
    }

    @PutMany(Languages.class)
    public void putManyLanguages(final Iterable<Languages> l, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages.class,
            Iterables.transform(l, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages languages) -> {
                return eq("platform", languages.getPlatform());
            });
    }

    @PutMany(LanguageStrings.class)
    public void putManyLanguageStrings(final Iterable<LanguageStrings> s, final PipelineContext context) {
        upsert(LanguageStrings.class, s, (final LanguageStrings strings) -> {
            return and(eq("platform", strings.getPlatform()), eq("locale", strings.getLocale()), eq("version", strings.getVersion()));
        });
    }

    @PutMany(LeagueList.class)
    public void putManyLeagueList(final Iterable<LeagueList> l, final PipelineContext context) {
        upsert(LeagueList.class, l, (final LeagueList list) -> {
            return and(eq("platform", list.getPlatform()), eq("leagueId", list.getLeagueId()));
        });
    }

    @PutMany(MapData.class)
    public void putManyMapData(final Iterable<MapData> d, final PipelineContext context) {
        upsert(MapData.class, d, (final MapData data) -> {
            return and(eq("platform", data.getPlatform()), eq("locale", data.getLocale()), eq("version", data.getVersion()));
        });
    }

    @PutMany(Mastery.class)
    public void putManyMastery(final Iterable<Mastery> m, final PipelineContext context) {
        upsert(Mastery.class, m, (final Mastery mastery) -> {
            return and(eq("platform", mastery.getPlatform()), eq("id", mastery.getId()), eq("version", mastery.getVersion()),
                eq("locale", mastery.getLocale()), eq("includedData", mastery.getIncludedData()));
        });
    }

    @PutMany(MasteryList.class)
    public void putManyMasteryList(final Iterable<MasteryList> l, final PipelineContext context) {
        upsert(MasteryList.class, l, (final MasteryList list) -> {
            return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                eq("includedData", list.getIncludedData()));
        });
        putManyMastery(
            Iterables.concat(
                StreamSupport.stream(l.spliterator(), false).map((final MasteryList masteries) -> masteries.getData().values()).collect(Collectors.toList())),
            context);
    }

    @PutMany(Match.class)
    public void putManyMatch(final Iterable<Match> m, final PipelineContext context) {
        upsert(Match.class, m, (final Match match) -> {
            return and(eq("platformId", match.getPlatformId()), eq("gameId", match.getGameId()));
        });
    }

    @PutMany(MatchTimeline.class)
    public void putManyMatchTimeline(final Iterable<MatchTimeline> t, final PipelineContext context) {
        upsert(MatchTimeline.class, t, (final MatchTimeline timeline) -> {
            return and(eq("platform", timeline.getPlatform()), eq("matchId", timeline.getMatchId()));
        });
    }

    @PutMany(ProfileIconData.class)
    public void putManyProfileIconData(final Iterable<ProfileIconData> d, final PipelineContext context) {
        upsert(ProfileIconData.class, d, (final ProfileIconData data) -> {
            return and(eq("platform", data.getPlatform()), eq("locale", data.getLocale()), eq("version", data.getVersion()));
        });
    }

    @PutMany(Realm.class)
    public void putManyRealm(final Iterable<Realm> r, final PipelineContext context) {
        upsert(Realm.class, r, (final Realm realm) -> {
            return eq("platform", realm.getPlatform());
        });
    }

    @PutMany(Rune.class)
    public void putManyRune(final Iterable<Rune> r, final PipelineContext context) {
        upsert(Rune.class, r, (final Rune rune) -> {
            return and(eq("platform", rune.getPlatform()), eq("id", rune.getId()), eq("version", rune.getVersion()),
                eq("locale", rune.getLocale()), eq("includedData", rune.getIncludedData()));
        });
    }

    @PutMany(RuneList.class)
    public void putManyRuneList(final Iterable<RuneList> l, final PipelineContext context) {
        upsert(RuneList.class, l, (final RuneList list) -> {
            return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                eq("includedData", list.getIncludedData()));
        });
        putManyRune(
            Iterables.concat(
                StreamSupport.stream(l.spliterator(), false).map((final RuneList runes) -> runes.getData().values()).collect(Collectors.toList())),
            context);
    }

    @PutMany(ShardStatus.class)
    public void putManyShardStatus(final Iterable<ShardStatus> s, final PipelineContext context) {
        upsert(ShardStatus.class, s, (final ShardStatus status) -> {
            return eq("region_tag", status.getRegion_tag());
        });
    }

    @PutMany(Summoner.class)
    public void putManySummoner(final Iterable<Summoner> s, final PipelineContext context) {
        upsert(Summoner.class, s, (final Summoner summoner) -> {
            return and(eq("platform", summoner.getPlatform()), eq("id", summoner.getId()));
        });
    }

    @PutMany(SummonerPositions.class)
    public void putManySummonerPositions(final Iterable<SummonerPositions> m, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions.class,
            Iterables.transform(m, com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions positions) -> {
                return and(eq("platform", positions.getPlatform()), eq("summonerId", positions.getSummonerId()));
            });
    }

    @PutMany(SummonerSpell.class)
    public void putManySummonerSpell(final Iterable<SummonerSpell> s, final PipelineContext context) {
        upsert(SummonerSpell.class, s, (final SummonerSpell spell) -> {
            return and(eq("platform", spell.getPlatform()), eq("id", spell.getId()), eq("version", spell.getVersion()),
                eq("locale", spell.getLocale()), eq("includedData", spell.getIncludedData()));
        });
    }

    @PutMany(SummonerSpellList.class)
    public void putManySummonerSpellList(final Iterable<SummonerSpellList> l, final PipelineContext context) {
        upsert(SummonerSpellList.class, l, (final SummonerSpellList list) -> {
            return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                eq("includedData", list.getIncludedData()));
        });
        putManySummonerSpell(
            Iterables.concat(
                StreamSupport.stream(l.spliterator(), false).map((final SummonerSpellList spells) -> spells.getData().values()).collect(Collectors.toList())),
            context);
    }

    @PutMany(TournamentMatches.class)
    public void putManyTournamentMatches(final Iterable<TournamentMatches> m, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches.class,
            Iterables.transform(m, com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches matches) -> {
                return and(eq("platform", matches.getPlatform()), eq("tournamentCode", matches.getTournamentCode()));
            });
    }

    @PutMany(Versions.class)
    public void putManyVersions(final Iterable<Versions> v, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions.class,
            Iterables.transform(v, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions versions) -> {
                return eq("platform", versions.getPlatform());
            });
    }

    @Put(MapData.class)
    public void putMapData(final MapData d, final PipelineContext context) {
        upsert(MapData.class, d, (final MapData data) -> {
            return and(eq("platform", data.getPlatform()), eq("locale", data.getLocale()), eq("version", data.getVersion()));
        });
    }

    @Put(Mastery.class)
    public void putMastery(final Mastery m, final PipelineContext context) {
        upsert(Mastery.class, m, (final Mastery mastery) -> {
            return and(eq("platform", mastery.getPlatform()), eq("id", mastery.getId()), eq("version", mastery.getVersion()),
                eq("locale", mastery.getLocale()), eq("includedData", mastery.getIncludedData()));
        });
    }

    @Put(MasteryList.class)
    public void putMasteryList(final MasteryList l, final PipelineContext context) {
        upsert(MasteryList.class, l, (final MasteryList list) -> {
            return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                eq("includedData", list.getIncludedData()));
        });
        putManyMastery(l.getData().values(), context);
    }

    @Put(Match.class)
    public void putMatch(final Match m, final PipelineContext context) {
        upsert(Match.class, m, (final Match match) -> {
            return and(eq("platformId", match.getPlatformId()), eq("gameId", match.getGameId()));
        });
    }

    @Put(MatchTimeline.class)
    public void putMatchTimeline(final MatchTimeline t, final PipelineContext context) {
        upsert(MatchTimeline.class, t, (final MatchTimeline timeline) -> {
            return and(eq("platform", timeline.getPlatform()), eq("matchId", timeline.getMatchId()));
        });
    }

    @Put(ProfileIconData.class)
    public void putProfileIconData(final ProfileIconData d, final PipelineContext context) {
        upsert(ProfileIconData.class, d, (final ProfileIconData data) -> {
            return and(eq("platform", data.getPlatform()), eq("locale", data.getLocale()), eq("version", data.getVersion()));
        });
    }

    @Put(Realm.class)
    public void putRealm(final Realm r, final PipelineContext context) {
        upsert(Realm.class, r, (final Realm realm) -> {
            return eq("platform", realm.getPlatform());
        });
    }

    @Put(Rune.class)
    public void putRune(final Rune r, final PipelineContext context) {
        upsert(Rune.class, r, (final Rune rune) -> {
            return and(eq("platform", rune.getPlatform()), eq("id", rune.getId()), eq("version", rune.getVersion()),
                eq("locale", rune.getLocale()), eq("includedData", rune.getIncludedData()));
        });
    }

    @Put(RuneList.class)
    public void putRuneList(final RuneList l, final PipelineContext context) {
        upsert(RuneList.class, l, (final RuneList list) -> {
            return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                eq("includedData", list.getIncludedData()));
        });
        putManyRune(l.getData().values(), context);
    }

    @Put(ShardStatus.class)
    public void putShardStatus(final ShardStatus s, final PipelineContext context) {
        upsert(ShardStatus.class, s, (final ShardStatus status) -> {
            return eq("region_tag", status.getRegion_tag());
        });
    }

    @Put(Summoner.class)
    public void putSummoner(final Summoner s, final PipelineContext context) {
        upsert(Summoner.class, s, (final Summoner summoner) -> {
            return and(eq("platform", summoner.getPlatform()), eq("id", summoner.getId()));
        });
    }

    @Put(SummonerPositions.class)
    public void putSummonerPositions(final SummonerPositions p, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions.convert(p),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions positions) -> {
                return and(eq("platform", positions.getPlatform()), eq("summonerId", positions.getSummonerId()));
            });
    }

    @Put(SummonerSpell.class)
    public void putSummonerSpell(final SummonerSpell s, final PipelineContext context) {
        upsert(SummonerSpell.class, s, (final SummonerSpell spell) -> {
            return and(eq("platform", spell.getPlatform()), eq("id", spell.getId()), eq("version", spell.getVersion()),
                eq("locale", spell.getLocale()), eq("includedData", spell.getIncludedData()));
        });
    }

    @Put(SummonerSpellList.class)
    public void putSummonerSpellList(final SummonerSpellList l, final PipelineContext context) {
        upsert(SummonerSpellList.class, l, (final SummonerSpellList list) -> {
            return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                eq("includedData", list.getIncludedData()));
        });
        putManySummonerSpell(l.getData().values(), context);
    }

    @Put(TournamentMatches.class)
    public void putTournamentMatches(final TournamentMatches m, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches.convert(m),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches matches) -> {
                return and(eq("platform", matches.getPlatform()), eq("tournamentCode", matches.getTournamentCode()));
            });
    }

    @Put(Versions.class)
    public void putVersions(final Versions v, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions.convert(v),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions versions) -> {
                return eq("platform", versions.getPlatform());
            });
    }
}

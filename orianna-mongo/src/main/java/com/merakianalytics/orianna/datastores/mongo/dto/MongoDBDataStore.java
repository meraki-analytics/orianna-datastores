package com.merakianalytics.orianna.datastores.mongo.dto;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.compoundIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import org.bson.codecs.pojo.AddOriannaIndexFields;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.merakianalytics.datapipelines.PipelineContext;
import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.merakianalytics.datapipelines.iterators.CloseableIterators;
import com.merakianalytics.datapipelines.sinks.Put;
import com.merakianalytics.datapipelines.sinks.PutMany;
import com.merakianalytics.datapipelines.sources.Get;
import com.merakianalytics.datapipelines.sources.GetMany;
import com.merakianalytics.orianna.datapipeline.common.QueryValidationException;
import com.merakianalytics.orianna.datapipeline.common.Utilities;
import com.merakianalytics.orianna.datapipeline.common.expiration.ExpirationPeriod;
import com.merakianalytics.orianna.datastores.mongo.FindResultIterator;
import com.merakianalytics.orianna.types.common.OriannaException;
import com.merakianalytics.orianna.types.common.Platform;
import com.merakianalytics.orianna.types.common.Queue;
import com.merakianalytics.orianna.types.common.Tier;
import com.merakianalytics.orianna.types.dto.champion.ChampionInfo;
import com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteries;
import com.merakianalytics.orianna.types.dto.championmastery.ChampionMastery;
import com.merakianalytics.orianna.types.dto.championmastery.ChampionMasteryScore;
import com.merakianalytics.orianna.types.dto.league.LeagueList;
import com.merakianalytics.orianna.types.dto.league.PositionalQueuesList;
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
import com.merakianalytics.orianna.types.dto.staticdata.MapDetails;
import com.merakianalytics.orianna.types.dto.staticdata.Mastery;
import com.merakianalytics.orianna.types.dto.staticdata.MasteryList;
import com.merakianalytics.orianna.types.dto.staticdata.Patch;
import com.merakianalytics.orianna.types.dto.staticdata.Patches;
import com.merakianalytics.orianna.types.dto.staticdata.ProfileIconData;
import com.merakianalytics.orianna.types.dto.staticdata.ProfileIconDetails;
import com.merakianalytics.orianna.types.dto.staticdata.Realm;
import com.merakianalytics.orianna.types.dto.staticdata.ReforgedRune;
import com.merakianalytics.orianna.types.dto.staticdata.ReforgedRunePath;
import com.merakianalytics.orianna.types.dto.staticdata.ReforgedRuneSlot;
import com.merakianalytics.orianna.types.dto.staticdata.ReforgedRuneTree;
import com.merakianalytics.orianna.types.dto.staticdata.Rune;
import com.merakianalytics.orianna.types.dto.staticdata.RuneList;
import com.merakianalytics.orianna.types.dto.staticdata.SummonerSpell;
import com.merakianalytics.orianna.types.dto.staticdata.SummonerSpellList;
import com.merakianalytics.orianna.types.dto.staticdata.Versions;
import com.merakianalytics.orianna.types.dto.status.ShardStatus;
import com.merakianalytics.orianna.types.dto.summoner.Summoner;
import com.merakianalytics.orianna.types.dto.thirdpartycode.VerificationString;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;

public class MongoDBDataStore extends com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore {
    public static class Configuration extends com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration {
        private static final Long DEFAULT_ETERNAL_PERIOD = -1L;
        private static final TimeUnit DEFAULT_ETERNAL_UNIT = TimeUnit.DAYS;
        private static final Map<String, ExpirationPeriod> DEFAULT_EXPIRATION_PERIODS = ImmutableMap.<String, ExpirationPeriod> builder()
            .put(ChampionInfo.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(ChampionMastery.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(ChampionMasteries.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(ChampionMasteryScore.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(LeagueList.class.getCanonicalName(), ExpirationPeriod.create(30L, TimeUnit.MINUTES))
            .put(SummonerPositions.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(PositionalQueuesList.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(Match.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            // TODO: Matchlist
            .put(MatchTimeline.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(TournamentMatches.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(CurrentGameInfo.class.getCanonicalName(), ExpirationPeriod.create(5L, TimeUnit.MINUTES))
            .put(FeaturedGames.class.getCanonicalName(), ExpirationPeriod.create(5L, TimeUnit.MINUTES))
            .put(Champion.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(ChampionList.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Item.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(ItemList.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Languages.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(LanguageStrings.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(MapData.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(MapDetails.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Mastery.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(MasteryList.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Patch.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Patches.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(ProfileIconData.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(ProfileIconDetails.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(ReforgedRune.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(ReforgedRuneTree.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Realm.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(Rune.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(RuneList.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(SummonerSpell.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(SummonerSpellList.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Versions.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(ShardStatus.class.getCanonicalName(), ExpirationPeriod.create(15L, TimeUnit.MINUTES))
            .put(Summoner.class.getCanonicalName(), ExpirationPeriod.create(1L, TimeUnit.DAYS))
            .put(VerificationString.class.getCanonicalName(), ExpirationPeriod.create(5L, TimeUnit.MINUTES))
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

    private static final Comparator<Patch> PATCH_COMPARATOR = (final Patch first, final Patch second) -> {
        final String[] firstParts = first.getName().replaceAll("[^\\d.]", "").split("\\.");
        final String[] secondParts = second.getName().replaceAll("[^\\d.]", "").split("\\.");
        for(int i = 0; i < Math.min(firstParts.length, secondParts.length); i++) {
            final int firstPart = Integer.parseInt(firstParts[i]);
            final int secondPart = Integer.parseInt(secondParts[i]);
            final int compare = Integer.compare(firstPart, secondPart);
            if(compare != 0) {
                return compare;
            }
        }
        return Integer.compare(first.getName().length(), second.getName().length());
    };

    private static String getCurrentVersion(final Platform platform, final PipelineContext context) {
        final Realm realm = context.getPipeline().get(Realm.class, ImmutableMap.<String, Object> of("platform", platform));
        return realm.getV();
    }

    private final Map<String, ExpirationPeriod> expirationPeriods;

    public MongoDBDataStore() {
        this(new Configuration());
    }

    public MongoDBDataStore(final Configuration config) {
        super(config);
        expirationPeriods = config.getExpirationPeriods();
        ensureIndexes();
    }

    private void ensureIndexes() {
        final Map<Class<?>, String[]> compositeKeys = ImmutableMap.<Class<?>, String[]> builder()
            .put(ChampionInfo.class, new String[] {"platform"})
            .put(ChampionMastery.class, new String[] {"platform", "summonerId", "championId"})
            .put(ChampionMasteries.class, new String[] {"platform", "summonerId"})
            .put(ChampionMasteryScore.class, new String[] {"platform", "summonerId"})
            .put(LeagueList.class, new String[] {"platform", "leagueId"})
            .put(SummonerPositions.class, new String[] {"platform", "summonerId"})
            .put(PositionalQueuesList.class, new String[] {"platform"})
            .put(Match.class, new String[] {"platformId", "gameId"})
            // TODO: Matchlist
            .put(MatchTimeline.class, new String[] {"platform", "matchId"})
            .put(TournamentMatches.class, new String[] {"platform", "tournamentCode"})
            .put(CurrentGameInfo.class, new String[] {"platformId", "summonerId"})
            .put(FeaturedGames.class, new String[] {"platform"})
            .put(Champion.class, new String[] {"platform", "id", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(Champion.class, new String[] {"platform", "name", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(Champion.class, new String[] {"platform", "key", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(ChampionList.class, new String[] {"platform", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(Item.class, new String[] {"platform", "id", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(Item.class, new String[] {"platform", "name", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(ItemList.class, new String[] {"platform", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(Languages.class, new String[] {"platform"})
            .put(LanguageStrings.class, new String[] {"platform", "version", "locale"})
            .put(MapData.class, new String[] {"platform", "version", "locale"})
            .put(MapDetails.class, new String[] {"platform", "mapId", "version", "locale"})
            .put(MapDetails.class, new String[] {"platform", "mapName", "version", "locale"})
            .put(Mastery.class, new String[] {"platform", "id", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(Mastery.class, new String[] {"platform", "name", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(MasteryList.class, new String[] {"platform", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(Patch.class, new String[] {"platform", "name"})
            .put(Patches.class, new String[] {"platform"})
            .put(ProfileIconData.class, new String[] {"platform", "version", "locale"})
            .put(ProfileIconDetails.class, new String[] {"platform", "id", "version", "locale"})
            .put(Realm.class, new String[] {"platform"})
            .put(ReforgedRune.class, new String[] {"platform", "id", "version", "locale"})
            .put(ReforgedRune.class, new String[] {"platform", "name", "version", "locale"})
            .put(ReforgedRune.class, new String[] {"platform", "key", "version", "locale"})
            .put(ReforgedRuneTree.class, new String[] {"platform", "version", "locale"})
            .put(Rune.class, new String[] {"platform", "id", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(Rune.class, new String[] {"platform", "name", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(RuneList.class, new String[] {"platform", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(SummonerSpell.class, new String[] {"platform", "id", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(SummonerSpell.class, new String[] {"platform", "name", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(SummonerSpellList.class, new String[] {"platform", "version", "locale", AddOriannaIndexFields.INCLUDED_DATA_HASH_FIELD_NAME})
            .put(Versions.class, new String[] {"platform"})
            .put(ShardStatus.class, new String[] {"platform"})
            .put(Summoner.class, new String[] {"platform", "puuid"})
            .put(Summoner.class, new String[] {"platform", "accountId"})
            .put(Summoner.class, new String[] {"platform", "id"})
            .put(Summoner.class, new String[] {"platform", "name"})
            .put(VerificationString.class, new String[] {"platform", "summonerId"})
            .build();

        for(final Class<?> clazz : compositeKeys.keySet()) {
            final MongoCollection<?> collection = getCollection(clazz);

            final String[] keys = compositeKeys.get(clazz);
            final Bson composite = compoundIndex(Arrays.stream(keys).map((final String key) -> ascending(key)).toArray(Bson[]::new));
            final IndexModel compositeKey = new IndexModel(composite, new IndexOptions().unique(true));

            List<IndexModel> indexes;

            final ExpirationPeriod period = expirationPeriods.get(clazz.getCanonicalName());
            if(period != null && period.getPeriod() > 0) {
                final IndexModel expiration =
                    new IndexModel(ascending(AddOriannaIndexFields.UPDATED_FIELD_NAME), new IndexOptions().expireAfter(period.getPeriod(), period.getUnit()));
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
    public Champion getChampion(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        final String key = (String)query.get("key");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name", key, "key");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final Bson filter;
        if(id != null) {
            filter = and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        } else if(name != null) {
            filter = and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        } else {
            filter = and(eq("platform", platform.getTag()), eq("key", key), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        }

        return findFirst(Champion.class, filter);
    }

    @Get(ChampionInfo.class)
    public ChampionInfo getChampionInfo(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        final Bson filter = eq("platform", platform.getTag());

        return findFirst(ChampionInfo.class, filter);
    }

    @SuppressWarnings("unchecked")
    @Get(ChampionList.class)
    public ChampionList getChampionList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");
        final Boolean dataById = query.get("dataById") == null ? Boolean.FALSE : (Boolean)query.get("dataById");

        final Bson filter = and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale), eq("includedData", includedData));

        final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ChampionList result =
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ChampionList.class, filter);

        if(result == null) {
            return null;
        }

        try(FindResultIterator<Champion> champions = find(Champion.class, filter)) {
            final ChampionList list = result.convert((int)champions.getCount(), dataById);
            while(champions.hasNext()) {
                final Champion champion = champions.next();
                list.getData().put(dataById ? Integer.toString(champion.getId()) : champion.getKey(), champion);
            }
            return list;
        }
    }

    @Get(ChampionMasteries.class)
    public ChampionMasteries getChampionMasteries(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        final com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries result =
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries.class,
                and(eq("platform", platform.getTag()), eq("summonerId", summonerId)));

        if(result == null) {
            return null;
        }

        try(FindResultIterator<ChampionMastery> masteries = find(ChampionMastery.class, and(eq("platform", platform.getTag()), eq("playerId", summonerId)))) {
            final ChampionMasteries list = result.convert((int)masteries.getCount());
            while(masteries.hasNext()) {
                list.add(masteries.next());
            }
            return list;
        }
    }

    @Get(ChampionMastery.class)
    public ChampionMastery getChampionMastery(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        final Number championId = (Number)query.get("championId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId", championId, "championId");

        return findFirst(ChampionMastery.class, and(eq("platform", platform.getTag()), eq("playerId", summonerId), eq("championId", championId)));
    }

    @Get(ChampionMasteryScore.class)
    public ChampionMasteryScore getChampionMasteryScore(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return findFirst(ChampionMasteryScore.class, and(eq("platform", platform.getTag()), eq("summonerId", summonerId)));
    }

    @Get(CurrentGameInfo.class)
    public CurrentGameInfo getCurrentGameInfo(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return findFirst(CurrentGameInfo.class, and(eq("platformId", platform.getTag()), eq("summonerId", summonerId)));
    }

    @Get(FeaturedGames.class)
    public FeaturedGames getFeaturedGames(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return findFirst(FeaturedGames.class, eq("platform", platform.getTag()));
    }

    @SuppressWarnings("unchecked")
    @Get(Item.class)
    public Item getItem(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final Bson filter;
        if(id != null) {
            filter = and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        } else {
            filter = and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        }

        return findFirst(Item.class, filter);
    }

    @SuppressWarnings("unchecked")
    @Get(ItemList.class)
    public ItemList getItemList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final Bson filter = and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale), eq("includedData", includedData));

        final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ItemList result =
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ItemList.class, filter);

        if(result == null) {
            return null;
        }

        try(FindResultIterator<Item> items = find(Item.class, filter)) {
            final ItemList list = result.convert((int)items.getCount());
            while(items.hasNext()) {
                final Item item = items.next();
                list.getData().put(Integer.toString(item.getId()), item);
            }
            return list;
        }
    }

    @Get(Languages.class)
    public Languages getLanguages(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return Optional
            .ofNullable(findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages.class, eq("platform", platform.getTag())))
            .map(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages::convert).orElse(null);
    }

    @Get(LanguageStrings.class)
    public LanguageStrings getLanguageStrings(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        return findFirst(LanguageStrings.class, and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale)));
    }

    @Get(LeagueList.class)
    public LeagueList getLeagueList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Tier tier = (Tier)query.get("tier");
        final Queue queue = (Queue)query.get("queue");
        final String leagueId = (String)query.get("leagueId");

        if(leagueId == null) {
            if(tier == null || queue == null) {
                throw new QueryValidationException("Query was missing required parameters! Either leagueId or tier and queue must be included!");
            } else if(!LEAGUE_LIST_ENDPOINTS.contains(tier)) {
                final StringBuilder sb = new StringBuilder();
                for(final Tier t : LEAGUE_LIST_ENDPOINTS) {
                    sb.append(", " + t);
                }
                throw new QueryValidationException("Query contained invalid parameters! tier must be one of [" + sb.substring(2) + "]!");
            } else if(!Queue.RANKED.contains(queue)) {
                final StringBuilder sb = new StringBuilder();
                for(final Queue qu : Queue.RANKED) {
                    sb.append(", " + qu);
                }
                throw new QueryValidationException("Query contained invalid parameters! queue must be one of [" + sb.substring(2) + "]!");
            }
        }

        final Bson filter;
        if(leagueId != null) {
            filter = and(eq("platform", platform.getTag()), eq("leagueId", leagueId));
        } else {
            filter = and(eq("platform", platform.getTag()), eq("tier", tier.name()), eq("queue", queue.name()));
        }

        return findFirst(LeagueList.class, filter);
    }

    @SuppressWarnings("unchecked")
    @GetMany(Champion.class)
    public CloseableIterator<Champion> getManyChampion(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        final Iterable<String> keys = (Iterable<String>)query.get("keys");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names", keys, "keys");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final FindQuery find;
        if(ids != null) {
            final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("id").build();
        } else if(names != null) {
            final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("name").build();
        } else {
            final List<BsonString> order = StreamSupport.stream(keys.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("key", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("key").build();
        }

        return find(Champion.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionInfo.class)
    public CloseableIterator<ChampionInfo> getManyChampionInfo(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        final List<BsonString> platforms =
            StreamSupport.stream(iter.spliterator(), false).map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
        final Bson filter = in("platform", platforms);
        final FindQuery find = FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();

        return find(ChampionInfo.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionList.class)
    public CloseableIterator<ChampionList> getManyChampionList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");
        final Boolean dataById = query.get("dataById") == null ? Boolean.FALSE : (Boolean)query.get("dataById");

        final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter =
            and(eq("platform", platform), in("version", versions), eq("locale", locale), eq("includedData", includedData));
        final FindQuery find = FindQuery.builder().filter(filter).order(versions).orderingField("version").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ChampionList> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ChampionList.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ChampionList result) -> {
            try(FindResultIterator<Champion> champions = find(Champion.class, filter)) {
                final ChampionList list = result.convert((int)champions.getCount(), dataById);
                while(champions.hasNext()) {
                    final Champion champion = champions.next();
                    list.getData().put(dataById ? Integer.toString(champion.getId()) : champion.getKey(), champion);
                }
                return list;
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMasteries.class)
    public CloseableIterator<ChampionMasteries> getManyChampionMasteries(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        final List<BsonNumber> summonerIds = numbersToBson(iter);
        final FindQuery find =
            FindQuery.builder().filter(and(eq("platform", platform), in("summonerId", summonerIds))).order(summonerIds).orderingField("summonerId").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results,
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries result) -> {
                try(FindResultIterator<ChampionMastery> masteries = find(ChampionMastery.class, and(eq("platform", platform), in("summonerId", summonerIds)))) {
                    final ChampionMasteries list = result.convert((int)masteries.getCount());
                    while(masteries.hasNext()) {
                        list.add(masteries.next());
                    }
                    return list;
                }
            });
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMastery.class)
    public CloseableIterator<ChampionMastery> getManyChampionMastery(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        final Iterable<Number> iter = (Iterable<Number>)query.get("championIds");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId", iter, "championIds");

        final List<BsonNumber> championIds = numbersToBson(iter);
        final Bson filter = and(eq("platform", platform), eq("playerId", summonerId), in("championId", championIds));
        final FindQuery find = FindQuery.builder().filter(filter).order(championIds).orderingField("championId").build();

        return find(ChampionMastery.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMasteryScore.class)
    public CloseableIterator<ChampionMasteryScore> getManyChampionMasteryScore(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        final List<BsonNumber> summonerIds = numbersToBson(iter);
        final Bson filter = and(eq("platform", platform), in("summonerId", summonerIds));
        final FindQuery find = FindQuery.builder().filter(filter).order(summonerIds).orderingField("summonerId").build();

        return find(ChampionMasteryScore.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(CurrentGameInfo.class)
    public CloseableIterator<CurrentGameInfo> getManyCurrentGameInfo(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        final List<BsonNumber> summonerIds = numbersToBson(iter);
        final Bson filter = and(eq("platformId", platform), in("summonerId", summonerIds));
        final FindQuery find = FindQuery.builder().filter(filter).order(summonerIds).orderingField("summonerId").build();

        return find(CurrentGameInfo.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(FeaturedGames.class)
    public CloseableIterator<FeaturedGames> getManyFeaturedGames(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        final List<BsonString> platforms =
            StreamSupport.stream(iter.spliterator(), false).map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
        final Bson filter = in("platform", platforms);
        final FindQuery find = FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();

        return find(FeaturedGames.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(Item.class)
    public CloseableIterator<Item> getManyItem(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final FindQuery find;
        if(ids != null) {
            final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("id").build();
        } else {
            final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("name").build();
        }

        return find(Item.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(ItemList.class)
    public CloseableIterator<ItemList> getManyItemList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter = and(eq("platform", platform), in("version", versions), eq("locale", locale), eq("includedData", includedData));
        final FindQuery find = FindQuery.builder().filter(filter).order(versions).orderingField("version").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ItemList> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ItemList.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ItemList result) -> {
            try(FindResultIterator<Item> items = find(Item.class, filter)) {
                final ItemList list = result.convert((int)items.getCount());
                while(items.hasNext()) {
                    final Item item = items.next();
                    list.getData().put(Integer.toString(item.getId()), item);
                }
                return list;
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Languages.class)
    public CloseableIterator<Languages> getManyLanguages(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        final List<BsonString> platforms =
            StreamSupport.stream(iter.spliterator(), false).map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
        final Bson filter = in("platform", platforms);
        final FindQuery find = FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages::convert);
    }

    @SuppressWarnings("unchecked")
    @GetMany(LanguageStrings.class)
    public CloseableIterator<LanguageStrings> getManyLanguageStrings(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("locales");
        Utilities.checkNotNull(platform, "platform", iter, "locales");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");

        final List<BsonString> locales = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter = and(eq("platform", platform), in("locale", locales), eq("version", version));
        final FindQuery find = FindQuery.builder().filter(filter).order(locales).orderingField("locale").build();

        return find(LanguageStrings.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(LeagueList.class)
    public CloseableIterator<LeagueList> getManyLeagueList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Tier tier = (Tier)query.get("tier");
        final Iterable<Queue> queues = (Iterable<Queue>)query.get("queues");
        final Iterable<String> leagueIds = (Iterable<String>)query.get("leagueIds");

        if(leagueIds == null) {
            if(tier == null || queues == null) {
                throw new QueryValidationException("Query was missing required parameters! Either leagueIds or tier and queues must be included!");
            } else if(!LEAGUE_LIST_ENDPOINTS.contains(tier)) {
                final StringBuilder sb = new StringBuilder();
                for(final Tier t : LEAGUE_LIST_ENDPOINTS) {
                    sb.append(", " + t);
                }
                throw new QueryValidationException("Query contained invalid parameters! tier must be one of [" + sb.substring(2) + "]!");
            }
        }

        final FindQuery find;
        if(leagueIds != null) {
            final List<BsonString> ids = StreamSupport.stream(leagueIds.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform), in("leagueId", ids));
            find = FindQuery.builder().filter(filter).order(ids).orderingField("leagueId").build();
        } else {
            final List<BsonString> ids =
                StreamSupport.stream(queues.spliterator(), false).map((final Queue queue) -> new BsonString(queue.name())).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform), eq("tier", tier.name()), in("queue", ids));
            find = FindQuery.builder().filter(filter).order(ids).orderingField("queue").build();
        }

        return find(LeagueList.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(MapData.class)
    public CloseableIterator<MapData> getManyMapData(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter = and(eq("platform", platform), in("version", versions), eq("locale", locale));
        final FindQuery find = FindQuery.builder().filter(filter).order(versions).orderingField("version").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MapData> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MapData.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MapData result) -> {
            try(FindResultIterator<MapDetails> maps = find(MapDetails.class, filter)) {
                final MapData data = result.convert((int)maps.getCount());
                while(maps.hasNext()) {
                    final MapDetails map = maps.next();
                    data.getData().put(Long.toString(map.getMapId()), map);
                }
                return data;
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(MapDetails.class)
    public CloseableIterator<MapDetails> getManyMapDetails(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final FindQuery find;
        if(ids != null) {
            final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform.getTag()), in("mapId", order), eq("version", version), eq("locale", locale));
            find = FindQuery.builder().filter(filter).order(order).orderingField("mapId").build();
        } else {
            final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale));
            find = FindQuery.builder().filter(filter).order(order).orderingField("name").build();
        }

        return find(MapDetails.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(Mastery.class)
    public CloseableIterator<Mastery> getManyMastery(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final FindQuery find;
        if(ids != null) {
            final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("id").build();
        } else {
            final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("name").build();
        }

        return find(Mastery.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(MasteryList.class)
    public CloseableIterator<MasteryList> getManyMasteryList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter = and(eq("platform", platform), in("version", versions), eq("locale", locale), eq("includedData", includedData));
        final FindQuery find = FindQuery.builder().filter(filter).order(versions).orderingField("version").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MasteryList> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MasteryList.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MasteryList result) -> {
            try(FindResultIterator<Mastery> masteries = find(Mastery.class, filter)) {
                final MasteryList list = result.convert((int)masteries.getCount());
                while(masteries.hasNext()) {
                    final Mastery mastery = masteries.next();
                    list.getData().put(Integer.toString(mastery.getId()), mastery);
                }
                return list;
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Match.class)
    public CloseableIterator<Match> getManyMatch(final Map<String, Object> query, final PipelineContext context) {
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

        final FindQuery find = FindQuery.builder().filter(filter).order(matchIds).orderingField("gameId").build();

        return find(Match.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(MatchTimeline.class)
    public CloseableIterator<MatchTimeline> getManyMatchTimeline(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("matchIds");
        Utilities.checkNotNull(platform, "platform", iter, "matchIds");

        final List<BsonNumber> matchIds = numbersToBson(iter);
        final Bson filter = and(eq("platform", platform), in("matchId", matchIds));
        final FindQuery find = FindQuery.builder().filter(filter).order(matchIds).orderingField("matchId").build();

        return find(MatchTimeline.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(Patch.class)
    public CloseableIterator<Patch> getManyPatch(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkNotNull(platform, "platform", names, "names");

        final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter = and(eq("platform", platform.getTag()), in("name", order));
        final FindQuery find = FindQuery.builder().filter(filter).order(order).orderingField("name").build();

        return find(Patch.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(Patches.class)
    public CloseableIterator<Patches> getManyPatches(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        final List<BsonString> platforms = StreamSupport.stream(iter.spliterator(), false)
            .map((final Platform platform) -> new BsonString(platform.getTag().toLowerCase())).collect(Collectors.toList());
        final Bson filter = in("platform", platforms);
        final FindQuery find = FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Patches> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Patches.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Patches result) -> {
            try(FindResultIterator<Patch> patches = find(Patch.class, filter)) {
                final Patches data = result.convert((int)patches.getCount());
                while(patches.hasNext()) {
                    data.getPatches().add(patches.next());
                }
                data.getPatches().sort(PATCH_COMPARATOR);
                return data;
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(PositionalQueuesList.class)
    public CloseableIterator<PositionalQueuesList> getManyPositionalQueuesList(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        final List<BsonString> platforms =
            StreamSupport.stream(iter.spliterator(), false).map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
        final Bson filter = in("platform", platforms);
        final FindQuery find = FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.PositionalQueuesList> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.PositionalQueuesList.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.PositionalQueuesList::convert);
    }

    @SuppressWarnings("unchecked")
    @GetMany(ProfileIconData.class)
    public CloseableIterator<ProfileIconData> getManyProfileIconData(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter = and(eq("platform", platform), in("version", versions), eq("locale", locale));
        final FindQuery find = FindQuery.builder().filter(filter).order(versions).orderingField("version").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ProfileIconData> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ProfileIconData.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ProfileIconData result) -> {
            try(FindResultIterator<ProfileIconDetails> icons = find(ProfileIconDetails.class, filter)) {
                final ProfileIconData data = result.convert((int)icons.getCount());
                while(icons.hasNext()) {
                    final ProfileIconDetails icon = icons.next();
                    data.getData().put(Long.toString(icon.getId()), icon);
                }
                return data;
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(ProfileIconDetails.class)
    public CloseableIterator<ProfileIconDetails> getManyProfileIconDetails(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        Utilities.checkNotNull(platform, "platform", ids, "ids");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
        final Bson filter = and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale));
        final FindQuery find = FindQuery.builder().filter(filter).order(order).orderingField("id").build();

        return find(ProfileIconDetails.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(Realm.class)
    public CloseableIterator<Realm> getManyRealm(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        final List<BsonString> platforms =
            StreamSupport.stream(iter.spliterator(), false).map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
        final Bson filter = in("platform", platforms);
        final FindQuery find = FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();

        return find(Realm.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(ReforgedRune.class)
    public CloseableIterator<ReforgedRune> getManyReforgedRune(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        final Iterable<String> keys = (Iterable<String>)query.get("keys");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names", keys, "keys");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final FindQuery find;
        if(ids != null) {
            final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale));
            find = FindQuery.builder().filter(filter).order(order).orderingField("id").build();
        } else if(names != null) {
            final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale));
            find = FindQuery.builder().filter(filter).order(order).orderingField("name").build();
        } else {
            final List<BsonString> order = StreamSupport.stream(keys.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("key", order), eq("version", version), eq("locale", locale));
            find = FindQuery.builder().filter(filter).order(order).orderingField("key").build();
        }

        return find(ReforgedRune.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(ReforgedRuneTree.class)
    public CloseableIterator<ReforgedRuneTree> getManyReforgedRuneTree(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter =
            and(eq("platform", platform), in("version", versions), eq("locale", locale));
        final FindQuery find = FindQuery.builder().filter(filter).order(versions).orderingField("version").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ReforgedRuneTree> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ReforgedRuneTree.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ReforgedRuneTree result) -> {
            try(FindResultIterator<ReforgedRune> runes = find(ReforgedRune.class, filter)) {
                final ReforgedRuneTree tree = result.convert();
                final Map<Integer, Map<Integer, ReforgedRuneSlot>> slots = new HashMap<>();
                for(final ReforgedRunePath path : tree) {
                    Map<Integer, ReforgedRuneSlot> forPath = slots.get(path.getId());
                    if(forPath == null) {
                        forPath = new HashMap<>();
                        slots.put(path.getId(), forPath);
                    }
                    for(int i = 0; i < path.getSlots().size(); i++) {
                        forPath.put(i, path.getSlots().get(i));
                    }
                }
                while(runes.hasNext()) {
                    final ReforgedRune rune = runes.next();
                    final ReforgedRuneSlot slot = slots.get(rune.getPathId()).get(rune.getSlot());
                    slot.getRunes().add(rune);
                }
                return tree;
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(Rune.class)
    public CloseableIterator<Rune> getManyRune(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final FindQuery find;
        if(ids != null) {
            final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("id").build();
        } else {
            final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("name").build();
        }

        return find(Rune.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(RuneList.class)
    public CloseableIterator<RuneList> getManyRuneList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter = and(eq("platform", platform), in("version", versions), eq("locale", locale), eq("includedData", includedData));
        final FindQuery find = FindQuery.builder().filter(filter).order(versions).orderingField("version").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.RuneList> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.RuneList.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.RuneList result) -> {
            try(FindResultIterator<Rune> runes = find(Rune.class, filter)) {
                final RuneList list = result.convert((int)runes.getCount());
                while(runes.hasNext()) {
                    final Rune rune = runes.next();
                    list.getData().put(Integer.toString(rune.getId()), rune);
                }
                return list;
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(ShardStatus.class)
    public CloseableIterator<ShardStatus> getManyShardStatus(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        final List<BsonString> platforms = StreamSupport.stream(iter.spliterator(), false)
            .map((final Platform platform) -> new BsonString(platform.getTag().toLowerCase())).collect(Collectors.toList());
        final Bson filter = in("region_tag", platforms);
        final FindQuery find = FindQuery.builder().filter(filter).order(platforms).orderingField("region_tag").build();

        return find(ShardStatus.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(Summoner.class)
    public CloseableIterator<Summoner> getManySummoner(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<String> puuids = (Iterable<String>)query.get("puuids");
        final Iterable<String> accountIds = (Iterable<String>)query.get("accountIds");
        final Iterable<String> summonerIds = (Iterable<String>)query.get("ids");
        final Iterable<String> summonerNames = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(puuids, "puuids", accountIds, "accountIds", summonerIds, "ids", summonerNames, "names");

        final FindQuery find;
        if(puuids != null) {
            final List<BsonString> order = StreamSupport.stream(puuids.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform.getTag()), in("puuid", order));
            find = FindQuery.builder().filter(filter).order(order).orderingField("puuid").build();
        } else if(accountIds != null) {
            final List<BsonString> order = StreamSupport.stream(accountIds.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform.getTag()), in("accountId", order));
            find = FindQuery.builder().filter(filter).order(order).orderingField("accountId").build();
        } else if(summonerIds != null) {
            final List<BsonString> order = StreamSupport.stream(summonerIds.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform.getTag()), in("id", order));
            find = FindQuery.builder().filter(filter).order(order).orderingField("id").build();
        } else {
            final List<BsonString> order = StreamSupport.stream(summonerNames.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter = and(eq("platform", platform.getTag()), in("name", order));
            find = FindQuery.builder().filter(filter).order(order).orderingField("name").build();
        }

        return find(Summoner.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerPositions.class)
    public CloseableIterator<SummonerPositions> getManySummonerPositions(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        final List<BsonNumber> summonerIds = numbersToBson(iter);
        final Bson filter = and(eq("platform", platform), in("summonerId", summonerIds));
        final FindQuery find = FindQuery.builder().filter(filter).order(summonerIds).orderingField("summonerId").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions::convert);
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerSpell.class)
    public CloseableIterator<SummonerSpell> getManySummonerSpell(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final FindQuery find;
        if(ids != null) {
            final List<BsonNumber> order = StreamSupport.stream(ids.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("id", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("id").build();
        } else {
            final List<BsonString> order = StreamSupport.stream(names.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
            final Bson filter =
                and(eq("platform", platform.getTag()), in("name", order), eq("version", version), eq("locale", locale), eq("includedData", includedData));
            find = FindQuery.builder().filter(filter).order(order).orderingField("name").build();
        }

        return find(SummonerSpell.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerSpellList.class)
    public CloseableIterator<SummonerSpellList> getManySummonerSpellList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final List<BsonString> versions = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter = and(eq("platform", platform), in("version", versions), eq("locale", locale), eq("includedData", includedData));
        final FindQuery find = FindQuery.builder().filter(filter).order(versions).orderingField("version").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.SummonerSpellList> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.SummonerSpellList.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.SummonerSpellList result) -> {
            try(FindResultIterator<SummonerSpell> spells = find(SummonerSpell.class, filter)) {
                final SummonerSpellList list = result.convert((int)spells.getCount());
                while(spells.hasNext()) {
                    final SummonerSpell spell = spells.next();
                    list.getData().put(Integer.toString(spell.getId()), spell);
                }
                return list;
            }
        });
    }

    @SuppressWarnings("unchecked")
    @GetMany(TournamentMatches.class)
    public CloseableIterator<TournamentMatches> getManyTournamentMatches(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("tournamentCodes");
        Utilities.checkNotNull(platform, "platform", iter, "tournamentCodes");

        final List<BsonString> tournamentCodes = StreamSupport.stream(iter.spliterator(), false).map(BsonString::new).collect(Collectors.toList());
        final Bson filter = and(eq("platform", platform), in("tournamentCode", tournamentCodes));
        final FindQuery find = FindQuery.builder().filter(filter).order(tournamentCodes).orderingField("tournamentCode").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches::convert);
    }

    @SuppressWarnings("unchecked")
    @GetMany(VerificationString.class)
    public CloseableIterator<VerificationString> getManyVerificationString(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        final List<BsonNumber> summonerIds = numbersToBson(iter);
        final Bson filter = and(eq("platform", platform), in("summonerId", summonerIds));
        final FindQuery find = FindQuery.builder().filter(filter).order(summonerIds).orderingField("summonerId").build();

        return find(VerificationString.class, find);
    }

    @SuppressWarnings("unchecked")
    @GetMany(Versions.class)
    public CloseableIterator<Versions> getManyVersions(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        final List<BsonString> platforms =
            StreamSupport.stream(iter.spliterator(), false).map((final Platform platform) -> new BsonString(platform.getTag())).collect(Collectors.toList());
        final Bson filter = in("platform", platforms);
        final FindQuery find = FindQuery.builder().filter(filter).order(platforms).orderingField("platform").build();

        final FindResultIterator<com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions> results =
            find(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions.class, find);

        if(results == null) {
            return null;
        }

        return CloseableIterators.transform(results, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions::convert);
    }

    @Get(MapData.class)
    public MapData getMapData(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final Bson filter = and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale));

        final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MapData result =
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MapData.class, filter);

        if(result == null) {
            return null;
        }

        try(FindResultIterator<MapDetails> maps = find(MapDetails.class, filter)) {
            final MapData data = result.convert((int)maps.getCount());
            while(maps.hasNext()) {
                final MapDetails map = maps.next();
                data.getData().put(Long.toString(map.getMapId()), map);
            }
            return data;
        }
    }

    @Get(MapDetails.class)
    public MapDetails getMapDetails(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final Bson filter;
        if(id != null) {
            filter = and(eq("platform", platform.getTag()), eq("mapId", id), eq("version", version), eq("locale", locale));
        } else {
            filter = and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale));
        }

        return findFirst(MapDetails.class, filter);
    }

    @SuppressWarnings("unchecked")
    @Get(Mastery.class)
    public Mastery getMastery(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final Bson filter;
        if(id != null) {
            filter = and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        } else {
            filter = and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        }

        return findFirst(Mastery.class, filter);
    }

    @SuppressWarnings("unchecked")
    @Get(MasteryList.class)
    public MasteryList getMasteryList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final Bson filter = and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale), eq("includedData", includedData));

        final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MasteryList result =
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MasteryList.class, filter);

        if(result == null) {
            return null;
        }

        try(FindResultIterator<Mastery> masteries = find(Mastery.class, filter)) {
            final MasteryList list = result.convert((int)masteries.getCount());
            while(masteries.hasNext()) {
                final Mastery mastery = masteries.next();
                list.getData().put(Integer.toString(mastery.getId()), mastery);
            }
            return list;
        }
    }

    @Get(Match.class)
    public Match getMatch(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number matchId = (Number)query.get("matchId");
        final String tournamentCode = (String)query.get("tournamentCode");
        Utilities.checkNotNull(platform, "platform", matchId, "matchId");

        final Bson filter;
        if(tournamentCode == null) {
            filter = and(eq("platformId", platform.getTag()), eq("gameId", matchId));
        } else {
            filter = and(eq("platformId", platform.getTag()), eq("gameId", matchId), eq("tournamentCode", tournamentCode));
        }

        return findFirst(Match.class, filter);
    }

    @Get(MatchTimeline.class)
    public MatchTimeline getMatchTimeline(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number matchId = (Number)query.get("matchId");
        Utilities.checkNotNull(platform, "platform", matchId, "matchId");

        final Bson filter = and(eq("platform", platform.getTag()), eq("matchId", matchId));

        return findFirst(MatchTimeline.class, filter);
    }

    @Get(Patch.class)
    public Patch getPatch(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final String name = (String)query.get("name");
        Utilities.checkNotNull(platform, "platform", name, "name");

        final Bson filter = and(eq("platform", platform.getTag()), eq("name", name));

        return findFirst(Patch.class, filter);
    }

    @Get(Patches.class)
    public Patches getPatches(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        final Bson filter = eq("platform", platform.getTag());

        final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Patches result =
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Patches.class, filter);

        if(result == null) {
            return null;
        }

        try(FindResultIterator<Patch> patches = find(Patch.class, filter)) {
            final Patches data = result.convert((int)patches.getCount());
            while(patches.hasNext()) {
                data.getPatches().add(patches.next());
            }
            data.getPatches().sort(PATCH_COMPARATOR);
            return data;
        }
    }

    @Get(PositionalQueuesList.class)
    public PositionalQueuesList getPositionalQueuesList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        final Bson filter = eq("platform", platform.getTag());

        return Optional.ofNullable(findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.PositionalQueuesList.class, filter))
            .map(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.PositionalQueuesList::convert).orElse(null);
    }

    @Get(ProfileIconData.class)
    public ProfileIconData getProfileIconData(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final Bson filter = and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale));

        final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ProfileIconData result =
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ProfileIconData.class, filter);

        if(result == null) {
            return null;
        }

        try(FindResultIterator<ProfileIconDetails> icons = find(ProfileIconDetails.class, filter)) {
            final ProfileIconData data = result.convert((int)icons.getCount());
            while(icons.hasNext()) {
                final ProfileIconDetails icon = icons.next();
                data.getData().put(Long.toString(icon.getId()), icon);
            }
            return data;
        }
    }

    @Get(ProfileIconDetails.class)
    public ProfileIconDetails getProfileIconDetails(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number id = (Number)query.get("id");
        Utilities.checkNotNull(platform, "platform", id, "id");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final Bson filter = and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale));

        return findFirst(ProfileIconDetails.class, filter);
    }

    @Get(Realm.class)
    public Realm getRealm(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        final Bson filter = eq("platform", platform.getTag());

        return findFirst(Realm.class, filter);
    }

    @Get(ReforgedRune.class)
    public ReforgedRune getReforgedRune(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        final String key = (String)query.get("key");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name", key, "key");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final Bson filter;
        if(id != null) {
            filter = and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale));
        } else if(name != null) {
            filter = and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale));
        } else {
            filter = and(eq("platform", platform.getTag()), eq("key", key), eq("version", version), eq("locale", locale));
        }

        return findFirst(ReforgedRune.class, filter);
    }

    @Get(ReforgedRuneTree.class)
    public ReforgedRuneTree getReforgedRuneTree(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");

        final Bson filter = and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale));

        final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ReforgedRuneTree result =
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ReforgedRuneTree.class, filter);

        if(result == null) {
            return null;
        }

        try(FindResultIterator<ReforgedRune> runes = find(ReforgedRune.class, filter)) {
            final ReforgedRuneTree tree = result.convert();
            final Map<Integer, Map<Integer, ReforgedRuneSlot>> slots = new HashMap<>();
            for(final ReforgedRunePath path : tree) {
                Map<Integer, ReforgedRuneSlot> forPath = slots.get(path.getId());
                if(forPath == null) {
                    forPath = new HashMap<>();
                    slots.put(path.getId(), forPath);
                }
                for(int i = 0; i < path.getSlots().size(); i++) {
                    forPath.put(i, path.getSlots().get(i));
                }
            }
            while(runes.hasNext()) {
                final ReforgedRune rune = runes.next();
                final ReforgedRuneSlot slot = slots.get(rune.getPathId()).get(rune.getSlot());
                slot.getRunes().add(rune);
            }
            return tree;
        }
    }

    @SuppressWarnings("unchecked")
    @Get(Rune.class)
    public Rune getRune(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final Bson filter;
        if(id != null) {
            filter = and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        } else {
            filter = and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        }

        return findFirst(Rune.class, filter);
    }

    @SuppressWarnings("unchecked")
    @Get(RuneList.class)
    public RuneList getRuneList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final Bson filter = and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale), eq("includedData", includedData));

        final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.RuneList result =
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.RuneList.class, filter);

        if(result == null) {
            return null;
        }

        try(FindResultIterator<Rune> runes = find(Rune.class, filter)) {
            final RuneList list = result.convert((int)runes.getCount());
            while(runes.hasNext()) {
                final Rune rune = runes.next();
                list.getData().put(Integer.toString(rune.getId()), rune);
            }
            return list;
        }
    }

    @Get(ShardStatus.class)
    public ShardStatus getShardStatus(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        final Bson filter = eq("region_tag", platform.getTag().toLowerCase());

        return findFirst(ShardStatus.class, filter);
    }

    @Get(Summoner.class)
    public Summoner getSummoner(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String puuid = (String)query.get("puuid");
        final String accountId = (String)query.get("accountId");
        final String summonerId = (String)query.get("id");
        final String summonerName = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(puuid, "puuid", accountId, "accountId", summonerId, "id", summonerName, "name");

        final Bson filter;
        if(puuid != null) {
            filter = and(eq("platform", platform.getTag()), eq("puuid", puuid));
        } else if(accountId != null) {
            filter = and(eq("platform", platform.getTag()), eq("accountId", accountId));
        } else if(summonerId != null) {
            filter = and(eq("platform", platform.getTag()), eq("id", summonerId));
        } else {
            filter = and(eq("platform", platform.getTag()), eq("name", summonerName));
        }

        return findFirst(Summoner.class, filter);
    }

    @Get(SummonerPositions.class)
    public SummonerPositions getSummonerPositions(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        final Bson filter = and(eq("platform", platform.getTag()), eq("summonerId", summonerId));

        return Optional.ofNullable(findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions.class, filter))
            .map(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions::convert).orElse(null);
    }

    @SuppressWarnings("unchecked")
    @Get(SummonerSpell.class)
    public SummonerSpell getSummonerSpell(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final Bson filter;
        if(id != null) {
            filter = and(eq("platform", platform.getTag()), eq("id", id), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        } else {
            filter = and(eq("platform", platform.getTag()), eq("name", name), eq("version", version), eq("locale", locale), eq("includedData", includedData));
        }

        return findFirst(SummonerSpell.class, filter);
    }

    @SuppressWarnings("unchecked")
    @Get(SummonerSpellList.class)
    public SummonerSpellList getSummonerSpellList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String version = query.get("version") == null ? getCurrentVersion(platform, context) : (String)query.get("version");
        final String locale = query.get("locale") == null ? platform.getDefaultLocale() : (String)query.get("locale");
        final Set<String> includedData = query.get("includedData") == null ? Collections.<String> emptySet() : (Set<String>)query.get("includedData");

        final Bson filter = and(eq("platform", platform.getTag()), eq("version", version), eq("locale", locale), eq("includedData", includedData));

        final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.SummonerSpellList result =
            findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.SummonerSpellList.class, filter);

        if(result == null) {
            return null;
        }

        try(FindResultIterator<SummonerSpell> spells = find(SummonerSpell.class, filter)) {
            final SummonerSpellList list = result.convert((int)spells.getCount());
            while(spells.hasNext()) {
                final SummonerSpell spell = spells.next();
                list.getData().put(Integer.toString(spell.getId()), spell);
            }
            return list;
        }
    }

    @Get(TournamentMatches.class)
    public TournamentMatches getTournamentMatches(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final String tournamentCode = (String)query.get("tournamentCode");
        Utilities.checkNotNull(platform, "platform", tournamentCode, "tournamentCode");

        final Bson filter = and(eq("platform", platform.getTag()), eq("tournamentCode", tournamentCode));

        return Optional.ofNullable(findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches.class, filter))
            .map(com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches::convert).orElse(null);
    }

    @Get(VerificationString.class)
    public VerificationString getVerificationString(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return findFirst(VerificationString.class, and(eq("platform", platform.getTag()), eq("summonerId", summonerId)));
    }

    @Get(Versions.class)
    public Versions getVersions(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        final Bson filter = eq("platform", platform.getTag());

        return Optional.ofNullable(findFirst(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions.class, filter))
            .map(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions::convert).orElse(null);
    }

    @Override
    protected Set<Class<?>> ignore() {
        final Set<String> included = new HashSet<>();
        for(final String name : expirationPeriods.keySet()) {
            if(expirationPeriods.get(name).getPeriod() != 0) {
                included.add(name);
            }
        }

        final Set<String> names = Sets.difference(Configuration.DEFAULT_EXPIRATION_PERIODS.keySet(), included);
        if(names.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<Class<?>> ignore = new HashSet<>();
        for(final String name : names) {
            try {
                ignore.add(Class.forName(name));
            } catch(final ClassNotFoundException e) {
                LOGGER.error("Failed to find class for name " + name + "!", e);
                throw new OriannaException("Failed to find class for name " + name + "! Report this to the orianna team.", e);
            }
        }
        return ignore;
    }

    @Put(Champion.class)
    public void putChampion(final Champion champion, final PipelineContext context) {
        upsert(Champion.class, champion, and(eq("platform", champion.getPlatform()), eq("id", champion.getId()), eq("version", champion.getVersion()),
            eq("locale", champion.getLocale()), eq("includedData", champion.getIncludedData())));
    }

    @Put(ChampionInfo.class)
    public void putChampionInfom(final ChampionInfo info, final PipelineContext context) {
        upsert(ChampionInfo.class, info, eq("platform", info.getPlatform()));
    }

    @Put(ChampionList.class)
    public void putChampionList(final ChampionList list, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ChampionList.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ChampionList.convert(list), and(eq("platform", list.getPlatform()),
                eq("version", list.getVersion()), eq("locale", list.getLocale()), eq("includedData", list.getIncludedData())));
        putManyChampion(list.getData().values(), context);
    }

    @Put(ChampionMasteries.class)
    public void putChampionMasteries(final ChampionMasteries masteries, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.championmastery.ChampionMasteries.convert(masteries),
            and(eq("platform", masteries.getPlatform()), eq("summonerId", masteries.getSummonerId())));
        putManyChampionMastery(masteries, context);
    }

    @Put(ChampionMastery.class)
    public void putChampionMastery(final ChampionMastery mastery, final PipelineContext context) {
        upsert(ChampionMastery.class, mastery,
            and(eq("platform", mastery.getPlatform()), eq("playerId", mastery.getSummonerId()), eq("championId", mastery.getChampionId())));
    }

    @Put(ChampionMasteryScore.class)
    public void putChampionMasteryScore(final ChampionMasteryScore score, final PipelineContext context) {
        upsert(ChampionMasteryScore.class, score, and(eq("platform", score.getPlatform()), eq("summonerId", score.getSummonerId())));
    }

    @Put(CurrentGameInfo.class)
    public void putCurrentGameInfo(final CurrentGameInfo info, final PipelineContext context) {
        upsert(CurrentGameInfo.class, info, and(eq("platformId", info.getPlatformId()), eq("summonerId", info.getSummonerId())));
    }

    @Put(FeaturedGames.class)
    public void putFeaturedGames(final FeaturedGames games, final PipelineContext context) {
        upsert(FeaturedGames.class, games, eq("platform", games.getPlatform()));
    }

    @Put(Item.class)
    public void putItem(final Item item, final PipelineContext context) {
        upsert(Item.class, item, and(eq("platform", item.getPlatform()), eq("id", item.getId()), eq("version", item.getVersion()),
            eq("locale", item.getLocale()), eq("includedData", item.getIncludedData())));
    }

    @Put(ItemList.class)
    public void putItemList(final ItemList list, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ItemList.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ItemList.convert(list), and(eq("platform", list.getPlatform()),
                eq("version", list.getVersion()), eq("locale", list.getLocale()), eq("includedData", list.getIncludedData())));
        putManyItem(list.getData().values(), context);
    }

    @Put(Languages.class)
    public void putLanguages(final Languages languages, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Languages.convert(languages), eq("platform", languages.getPlatform()));
    }

    @Put(LanguageStrings.class)
    public void putLanguageStrings(final LanguageStrings strings, final PipelineContext context) {
        upsert(LanguageStrings.class, strings,
            and(eq("platform", strings.getPlatform()), eq("locale", strings.getLocale()), eq("version", strings.getVersion())));
    }

    @Put(LeagueList.class)
    public void putLeagueList(final LeagueList league, final PipelineContext context) {
        upsert(LeagueList.class, league, and(eq("platform", league.getPlatform()), eq("leagueId", league.getLeagueId())));
    }

    @PutMany(Champion.class)
    public void putManyChampion(final Iterable<Champion> c, final PipelineContext context) {
        upsert(Champion.class, c, (final Champion champion) -> {
            return and(eq("platform", champion.getPlatform()), eq("id", champion.getId()), eq("version", champion.getVersion()),
                eq("locale", champion.getLocale()), eq("includedData", champion.getIncludedData()));
        });
    }

    @PutMany(ChampionInfo.class)
    public void putManyChampionInfo(final Iterable<ChampionInfo> i, final PipelineContext context) {
        upsert(ChampionInfo.class, i, (final ChampionInfo info) -> {
            return eq("platform", info.getPlatform());
        });
    }

    @PutMany(ChampionList.class)
    public void putManyChampionList(final Iterable<ChampionList> l, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ChampionList.class,
            Iterables.transform(l, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ChampionList::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ChampionList list) -> {
                return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                    eq("includedData", list.getIncludedData()));
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
            return and(eq("platform", mastery.getPlatform()), eq("playerId", mastery.getSummonerId()), eq("championId", mastery.getChampionId()));
        });
    }

    @PutMany(ChampionMasteryScore.class)
    public void putManyChampionMasteryScore(final Iterable<ChampionMasteryScore> m, final PipelineContext context) {
        upsert(ChampionMasteryScore.class, m, (final ChampionMasteryScore masteries) -> {
            return and(eq("platform", masteries.getPlatform()), eq("summonerId", masteries.getSummonerId()));
        });
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
            return and(eq("platform", item.getPlatform()), eq("id", item.getId()), eq("version", item.getVersion()), eq("locale", item.getLocale()),
                eq("includedData", item.getIncludedData()));
        });
    }

    @PutMany(ItemList.class)
    public void putManyItemList(final Iterable<ItemList> l, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ItemList.class,
            Iterables.transform(l, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ItemList::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ItemList list) -> {
                return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                    eq("includedData", list.getIncludedData()));
            });
        putManyItem(
            Iterables.concat(StreamSupport.stream(l.spliterator(), false).map((final ItemList items) -> items.getData().values()).collect(Collectors.toList())),
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
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MapData.class,
            Iterables.transform(d, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MapData::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MapData data) -> {
                return and(eq("platform", data.getPlatform()), eq("version", data.getVersion()), eq("locale", data.getLocale()));
            });
        putManyMapDetails(
            Iterables.concat(StreamSupport.stream(d.spliterator(), false).map((final MapData data) -> data.getData().values()).collect(Collectors.toList())),
            context);
    }

    @PutMany(MapDetails.class)
    public void putManyMapDetails(final Iterable<MapDetails> m, final PipelineContext context) {
        upsert(MapDetails.class, m, (final MapDetails map) -> {
            return and(eq("platform", map.getPlatform()), eq("mapId", map.getMapId()), eq("version", map.getVersion()), eq("locale", map.getLocale()));
        });
    }

    @PutMany(Mastery.class)
    public void putManyMastery(final Iterable<Mastery> m, final PipelineContext context) {
        upsert(Mastery.class, m, (final Mastery mastery) -> {
            return and(eq("platform", mastery.getPlatform()), eq("id", mastery.getId()), eq("version", mastery.getVersion()), eq("locale", mastery.getLocale()),
                eq("includedData", mastery.getIncludedData()));
        });
    }

    @PutMany(MasteryList.class)
    public void putManyMasteryList(final Iterable<MasteryList> l, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MasteryList.class,
            Iterables.transform(l, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MasteryList::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MasteryList list) -> {
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

    @PutMany(Patch.class)
    public void putManyPatch(final Iterable<Patch> p, final PipelineContext context) {
        upsert(Patch.class, p, (final Patch patch) -> {
            return and(eq("platform", patch.getPlatform()), eq("name", patch.getName()));
        });
    }

    @PutMany(Patches.class)
    public void putManyPatches(final Iterable<Patches> d, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Patches.class,
            Iterables.transform(d, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Patches::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Patches data) -> {
                return eq("platform", data.getPlatform());
            });
        putManyPatch(
            Iterables.concat(StreamSupport.stream(d.spliterator(), false).map((final Patches data) -> data.getPatches()).collect(Collectors.toList())),
            context);
    }

    @PutMany(PositionalQueuesList.class)
    public void putManyPositionalQueuesList(final Iterable<PositionalQueuesList> q, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.PositionalQueuesList.class,
            Iterables.transform(q, com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.PositionalQueuesList::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.PositionalQueuesList queues) -> {
                return eq("platform", queues.getPlatform());
            });
    }

    @PutMany(ProfileIconData.class)
    public void putManyProfileIconData(final Iterable<ProfileIconData> d, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ProfileIconData.class,
            Iterables.transform(d, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ProfileIconData::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ProfileIconData data) -> {
                return and(eq("platform", data.getPlatform()), eq("version", data.getVersion()), eq("locale", data.getLocale()));
            });
        putManyProfileIconDetails(
            Iterables
                .concat(StreamSupport.stream(d.spliterator(), false).map((final ProfileIconData data) -> data.getData().values()).collect(Collectors.toList())),
            context);
    }

    @PutMany(ProfileIconDetails.class)
    public void putManyProfileIconDetails(final Iterable<ProfileIconDetails> i, final PipelineContext context) {
        upsert(ProfileIconDetails.class, i, (final ProfileIconDetails icon) -> {
            return and(eq("platform", icon.getPlatform()), eq("id", icon.getId()), eq("version", icon.getVersion()), eq("locale", icon.getLocale()));
        });
    }

    @PutMany(Realm.class)
    public void putManyRealm(final Iterable<Realm> r, final PipelineContext context) {
        upsert(Realm.class, r, (final Realm realm) -> {
            return eq("platform", realm.getPlatform());
        });
    }

    @PutMany(ReforgedRune.class)
    public void putManyReforgedRune(final Iterable<ReforgedRune> r, final PipelineContext context) {
        upsert(ReforgedRune.class, r, (final ReforgedRune rune) -> {
            return and(eq("platform", rune.getPlatform()), eq("id", rune.getId()), eq("version", rune.getVersion()), eq("locale", rune.getLocale()));
        });
    }

    @PutMany(ReforgedRuneTree.class)
    public void putManyReforgedRuneTree(final Iterable<ReforgedRuneTree> t, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ReforgedRuneTree.class,
            Iterables.transform(t, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ReforgedRuneTree::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ReforgedRuneTree tree) -> {
                return and(eq("platform", tree.getPlatform()), eq("version", tree.getVersion()), eq("locale", tree.getLocale()));
            });
        putManyReforgedRune(
            Iterables.concat(
                StreamSupport.stream(t.spliterator(), false)
                    .map((final ReforgedRuneTree tree) -> tree.stream().map(ReforgedRunePath::getSlots).flatMap(List::stream).map(ReforgedRuneSlot::getRunes)
                        .flatMap(List::stream).collect(Collectors.toList()))
                    .collect(Collectors.toList())),
            context);
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
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.RuneList.class,
            Iterables.transform(l, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.RuneList::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.RuneList list) -> {
                return and(eq("platform", list.getPlatform()), eq("version", list.getVersion()), eq("locale", list.getLocale()),
                    eq("includedData", list.getIncludedData()));
            });
        putManyRune(
            Iterables.concat(StreamSupport.stream(l.spliterator(), false).map((final RuneList runes) -> runes.getData().values()).collect(Collectors.toList())),
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
            return and(eq("platform", summoner.getPlatform()), eq("puuid", summoner.getPuuid()));
        });
    }

    @PutMany(SummonerPositions.class)
    public void putManySummonerPositions(final Iterable<SummonerPositions> p, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions.class,
            Iterables.transform(p, com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions positions) -> {
                return and(eq("platform", positions.getPlatform()), eq("summonerId", positions.getSummonerId()));
            });
    }

    @PutMany(SummonerSpell.class)
    public void putManySummonerSpell(final Iterable<SummonerSpell> s, final PipelineContext context) {
        upsert(SummonerSpell.class, s, (final SummonerSpell spell) -> {
            return and(eq("platform", spell.getPlatform()), eq("id", spell.getId()), eq("version", spell.getVersion()), eq("locale", spell.getLocale()),
                eq("includedData", spell.getIncludedData()));
        });
    }

    @PutMany(SummonerSpellList.class)
    public void putManySummonerSpellList(final Iterable<SummonerSpellList> l, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.SummonerSpellList.class,
            Iterables.transform(l, com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.SummonerSpellList::convert),
            (final com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.SummonerSpellList list) -> {
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

    @PutMany(VerificationString.class)
    public void putManyVerificationString(final Iterable<VerificationString> s, final PipelineContext context) {
        upsert(VerificationString.class, s, (final VerificationString string) -> {
            return and(eq("platform", string.getPlatform()), eq("summonerId", string.getSummonerId()));
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
    public void putMapData(final MapData data, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MapData.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MapData.convert(data),
            and(eq("platform", data.getPlatform()), eq("version", data.getVersion()), eq("locale", data.getLocale())));
        putManyMapDetails(data.getData().values(), context);
    }

    @Put(MapDetails.class)
    public void putMapDetails(final MapDetails map, final PipelineContext context) {
        upsert(MapDetails.class, map,
            and(eq("platform", map.getPlatform()), eq("mapId", map.getMapId()), eq("icon", map.getVersion()), eq("locale", map.getLocale())));
    }

    @Put(Mastery.class)
    public void putMastery(final Mastery mastery, final PipelineContext context) {
        upsert(Mastery.class, mastery, and(eq("platform", mastery.getPlatform()), eq("id", mastery.getId()), eq("version", mastery.getVersion()),
            eq("locale", mastery.getLocale()), eq("includedData", mastery.getIncludedData())));
    }

    @Put(MasteryList.class)
    public void putMasteryList(final MasteryList list, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MasteryList.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.MasteryList.convert(list), and(eq("platform", list.getPlatform()),
                eq("version", list.getVersion()), eq("locale", list.getLocale()), eq("includedData", list.getIncludedData())));
        putManyMastery(list.getData().values(), context);
    }

    @Put(Match.class)
    public void putMatch(final Match match, final PipelineContext context) {
        upsert(Match.class, match, and(eq("platformId", match.getPlatformId()), eq("gameId", match.getGameId())));
    }

    @Put(MatchTimeline.class)
    public void putMatchTimeline(final MatchTimeline timeline, final PipelineContext context) {
        upsert(MatchTimeline.class, timeline, and(eq("platform", timeline.getPlatform()), eq("matchId", timeline.getMatchId())));
    }

    @Put(Patch.class)
    public void putPatch(final Patch patch, final PipelineContext context) {
        upsert(Patch.class, patch, and(eq("platform", patch.getPlatform()), eq("name", patch.getName())));
    }

    @Put(Patches.class)
    public void putPatches(final Patches data, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Patches.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Patches.convert(data), eq("platform", data.getPlatform()));
        putManyPatch(data.getPatches(), context);
    }

    @Put(PositionalQueuesList.class)
    public void putPositionalQueuesList(final PositionalQueuesList queues, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.PositionalQueuesList.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.PositionalQueuesList.convert(queues), eq("platform", queues.getPlatform()));
    }

    @Put(ProfileIconData.class)
    public void putProfileIconData(final ProfileIconData data, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ProfileIconData.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ProfileIconData.convert(data),
            and(eq("platform", data.getPlatform()), eq("version", data.getVersion()), eq("locale", data.getLocale())));
        putManyProfileIconDetails(data.getData().values(), context);
    }

    @Put(ProfileIconDetails.class)
    public void putProfileIconDetails(final ProfileIconDetails icon, final PipelineContext context) {
        upsert(ProfileIconDetails.class, icon,
            and(eq("platform", icon.getPlatform()), eq("id", icon.getId()), eq("icon", icon.getVersion()), eq("locale", icon.getLocale())));
    }

    @Put(Realm.class)
    public void putRealm(final Realm realm, final PipelineContext context) {
        upsert(Realm.class, realm, eq("platform", realm.getPlatform()));
    }

    @Put(ReforgedRune.class)
    public void putReforgedRune(final ReforgedRune rune, final PipelineContext context) {
        upsert(ReforgedRune.class, rune, and(eq("platform", rune.getPlatform()), eq("id", rune.getId()), eq("version", rune.getVersion()),
            eq("locale", rune.getLocale())));
    }

    @Put(ReforgedRuneTree.class)
    public void putReforgedRuneTree(final ReforgedRuneTree tree, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ReforgedRuneTree.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.ReforgedRuneTree.convert(tree), and(eq("platform", tree.getPlatform()),
                eq("version", tree.getVersion()), eq("locale", tree.getLocale())));
        final List<ReforgedRune> runes = tree.stream().map(ReforgedRunePath::getSlots).flatMap(List::stream).map(ReforgedRuneSlot::getRunes)
            .flatMap(List::stream).collect(Collectors.toList());
        putManyReforgedRune(runes, context);
    }

    @Put(Rune.class)
    public void putRune(final Rune rune, final PipelineContext context) {
        upsert(Rune.class, rune, and(eq("platform", rune.getPlatform()), eq("id", rune.getId()), eq("version", rune.getVersion()),
            eq("locale", rune.getLocale()), eq("includedData", rune.getIncludedData())));
    }

    @Put(RuneList.class)
    public void putRuneList(final RuneList list, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.RuneList.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.RuneList.convert(list), and(eq("platform", list.getPlatform()),
                eq("version", list.getVersion()), eq("locale", list.getLocale()), eq("includedData", list.getIncludedData())));
        putManyRune(list.getData().values(), context);
    }

    @Put(ShardStatus.class)
    public void putShardStatus(final ShardStatus status, final PipelineContext context) {
        upsert(ShardStatus.class, status, eq("region_tag", status.getRegion_tag()));
    }

    @Put(Summoner.class)
    public void putSummoner(final Summoner summoner, final PipelineContext context) {
        upsert(Summoner.class, summoner, and(eq("platform", summoner.getPlatform()), eq("puuid", summoner.getPuuid())));
    }

    @Put(SummonerPositions.class)
    public void putSummonerPositions(final SummonerPositions positions, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.league.SummonerPositions.convert(positions),
            and(eq("platform", positions.getPlatform()), eq("summonerId", positions.getSummonerId())));
    }

    @Put(SummonerSpell.class)
    public void putSummonerSpell(final SummonerSpell spell, final PipelineContext context) {
        upsert(SummonerSpell.class, spell, and(eq("platform", spell.getPlatform()), eq("id", spell.getId()), eq("version", spell.getVersion()),
            eq("locale", spell.getLocale()), eq("includedData", spell.getIncludedData())));
    }

    @Put(SummonerSpellList.class)
    public void putSummonerSpellList(final SummonerSpellList list, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.SummonerSpellList.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.SummonerSpellList.convert(list), and(eq("platform", list.getPlatform()),
                eq("version", list.getVersion()), eq("locale", list.getLocale()), eq("includedData", list.getIncludedData())));
        putManySummonerSpell(list.getData().values(), context);
    }

    @Put(TournamentMatches.class)
    public void putTournamentMatches(final TournamentMatches matches, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.match.TournamentMatches.convert(matches),
            and(eq("platform", matches.getPlatform()), eq("tournamentCode", matches.getTournamentCode())));
    }

    @Put(VerificationString.class)
    public void putVerificationString(final VerificationString string, final PipelineContext context) {
        upsert(VerificationString.class, string, and(eq("platform", string.getPlatform()), eq("summonerId", string.getSummonerId())));
    }

    @Put(Versions.class)
    public void putVersions(final Versions versions, final PipelineContext context) {
        upsert(com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions.class,
            com.merakianalytics.orianna.datastores.mongo.proxies.dto.staticdata.Versions.convert(versions), eq("platform", versions.getPlatform()));
    }
}

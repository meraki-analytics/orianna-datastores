package com.merakianalytics.orianna.datastores.xodus.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import com.merakianalytics.orianna.types.UniqueKeys;
import com.merakianalytics.orianna.types.common.OriannaException;
import com.merakianalytics.orianna.types.common.Platform;
import com.merakianalytics.orianna.types.common.Queue;
import com.merakianalytics.orianna.types.common.Tier;
import com.merakianalytics.orianna.types.data.CoreData;
import com.merakianalytics.orianna.types.data.champion.ChampionRotation;
import com.merakianalytics.orianna.types.data.championmastery.ChampionMasteries;
import com.merakianalytics.orianna.types.data.championmastery.ChampionMastery;
import com.merakianalytics.orianna.types.data.championmastery.ChampionMasteryScore;
import com.merakianalytics.orianna.types.data.league.League;
import com.merakianalytics.orianna.types.data.league.LeaguePositions;
import com.merakianalytics.orianna.types.data.match.Match;
import com.merakianalytics.orianna.types.data.match.Timeline;
import com.merakianalytics.orianna.types.data.match.TournamentMatches;
import com.merakianalytics.orianna.types.data.spectator.CurrentMatch;
import com.merakianalytics.orianna.types.data.spectator.FeaturedMatches;
import com.merakianalytics.orianna.types.data.staticdata.Champion;
import com.merakianalytics.orianna.types.data.staticdata.Champions;
import com.merakianalytics.orianna.types.data.staticdata.Item;
import com.merakianalytics.orianna.types.data.staticdata.Items;
import com.merakianalytics.orianna.types.data.staticdata.LanguageStrings;
import com.merakianalytics.orianna.types.data.staticdata.Languages;
import com.merakianalytics.orianna.types.data.staticdata.Map;
import com.merakianalytics.orianna.types.data.staticdata.Maps;
import com.merakianalytics.orianna.types.data.staticdata.Masteries;
import com.merakianalytics.orianna.types.data.staticdata.Mastery;
import com.merakianalytics.orianna.types.data.staticdata.Patch;
import com.merakianalytics.orianna.types.data.staticdata.Patches;
import com.merakianalytics.orianna.types.data.staticdata.ProfileIcon;
import com.merakianalytics.orianna.types.data.staticdata.ProfileIcons;
import com.merakianalytics.orianna.types.data.staticdata.Realm;
import com.merakianalytics.orianna.types.data.staticdata.ReforgedRune;
import com.merakianalytics.orianna.types.data.staticdata.ReforgedRunes;
import com.merakianalytics.orianna.types.data.staticdata.Rune;
import com.merakianalytics.orianna.types.data.staticdata.Runes;
import com.merakianalytics.orianna.types.data.staticdata.SummonerSpell;
import com.merakianalytics.orianna.types.data.staticdata.SummonerSpells;
import com.merakianalytics.orianna.types.data.staticdata.Versions;
import com.merakianalytics.orianna.types.data.status.ShardStatus;
import com.merakianalytics.orianna.types.data.summoner.Summoner;
import com.merakianalytics.orianna.types.data.thirdpartycode.VerificationString;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.CompoundByteIterable;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;

public class XodusDataStore extends com.merakianalytics.orianna.datastores.xodus.XodusDataStore {
    public static class Configuration extends com.merakianalytics.orianna.datastores.xodus.XodusDataStore.Configuration {
        private static final Long DEFAULT_ETERNAL_PERIOD = -1L;
        private static final TimeUnit DEFAULT_ETERNAL_UNIT = TimeUnit.DAYS;
        private static final java.util.Map<String, ExpirationPeriod> DEFAULT_EXPIRATION_PERIODS = ImmutableMap.<String, ExpirationPeriod> builder()
            .put(ChampionRotation.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(ChampionMastery.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(ChampionMasteries.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(ChampionMasteryScore.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(League.class.getCanonicalName(), ExpirationPeriod.create(30L, TimeUnit.MINUTES))
            .put(LeaguePositions.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(Match.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            // TODO: Matchlist
            .put(Timeline.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(TournamentMatches.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(CurrentMatch.class.getCanonicalName(), ExpirationPeriod.create(5L, TimeUnit.MINUTES))
            .put(FeaturedMatches.class.getCanonicalName(), ExpirationPeriod.create(5L, TimeUnit.MINUTES))
            .put(Champion.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Champions.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Item.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Items.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Languages.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(LanguageStrings.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Maps.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Map.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Mastery.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Masteries.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Patch.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Patches.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(ProfileIcons.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(ProfileIcon.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(ReforgedRune.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(ReforgedRunes.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Realm.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(Rune.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Runes.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(SummonerSpell.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(SummonerSpells.class.getCanonicalName(), ExpirationPeriod.create(DEFAULT_ETERNAL_PERIOD, DEFAULT_ETERNAL_UNIT))
            .put(Versions.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(ShardStatus.class.getCanonicalName(), ExpirationPeriod.create(15L, TimeUnit.MINUTES))
            .put(Summoner.class.getCanonicalName(), ExpirationPeriod.create(1L, TimeUnit.DAYS))
            .put(VerificationString.class.getCanonicalName(), ExpirationPeriod.create(5L, TimeUnit.MINUTES))
            .build();

        private java.util.Map<String, ExpirationPeriod> expirationPeriods = DEFAULT_EXPIRATION_PERIODS;

        /**
         * @return the expirationPeriods
         */
        public java.util.Map<String, ExpirationPeriod> getExpirationPeriods() {
            return expirationPeriods;
        }

        /**
         * @param expirationPeriods
         *        the expirationPeriods to set
         */
        public void setExpirationPeriods(final java.util.Map<String, ExpirationPeriod> expirationPeriods) {
            this.expirationPeriods = expirationPeriods;
        }
    }

    private static final Set<Tier> LEAGUE_LIST_ENDPOINTS = ImmutableSet.of(Tier.MASTER, Tier.CHALLENGER);
    private static final Logger LOGGER = LoggerFactory.getLogger(XodusDataStore.class);
    private static final StoreConfig STORE_CONFIG = StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;

    private static String getCurrentVersion(final Platform platform, final PipelineContext context) {
        final Realm realm = context.getPipeline().get(Realm.class, ImmutableMap.<String, Object> of("platform", platform));
        return realm.getVersion();
    }

    private static <T extends CoreData> ByteIterable toByteIterable(final T value) {
        return new CompoundByteIterable(new ByteIterable[] {LongBinding.longToEntry(System.currentTimeMillis()), new ArrayByteIterable(value.toBytes())}, 2);
    }

    private final java.util.Map<String, ExpirationPeriod> expirationPeriods;

    public XodusDataStore() {
        this(new Configuration());
    }

    public XodusDataStore(final Configuration config) {
        super(config);
        expirationPeriods = config.getExpirationPeriods();
    }

    private <T extends CoreData> T get(final Class<T> clazz, final int key) {
        final Transaction transaction = xodus.beginTransaction();
        ByteIterable data = null;
        try {
            final Store store = xodus.openStore(clazz.getCanonicalName(), STORE_CONFIG, transaction);
            final ByteIterable result = store.get(transaction, IntegerBinding.intToEntry(key));

            if(result == null) {
                return null;
            }

            final ByteIterable timestamp = result.subIterable(0, 8);
            final long stored = LongBinding.entryToLong(timestamp);

            final ExpirationPeriod period = expirationPeriods.get(clazz.getCanonicalName());
            if(period != null && period.getPeriod() > 0) {
                final long expiration = stored + period.getUnit().toMillis(period.getPeriod());
                if(System.currentTimeMillis() >= expiration) {
                    store.delete(transaction, IntegerBinding.intToEntry(key));
                    return null;
                }
            }

            data = result.subIterable(8, result.getLength() - 8);
        } finally {
            transaction.commit();
        }

        return CoreData.fromBytes(clazz, data.getBytesUnsafe());
    }

    private <T extends CoreData> CloseableIterator<T> get(final Class<T> clazz, final Iterator<Integer> keyIterator) {
        final List<Integer> keys = Lists.newArrayList(keyIterator);
        final ExpirationPeriod period = expirationPeriods.get(clazz.getCanonicalName());
        final List<ByteIterable> results = new ArrayList<>(keys.size());

        final Transaction transaction = xodus.beginTransaction();
        try {
            final Store store = xodus.openStore(clazz.getCanonicalName(), STORE_CONFIG, transaction);

            for(final Integer key : keys) {
                final ByteIterable result = store.get(transaction, IntegerBinding.intToEntry(key));

                if(result == null) {
                    return null;
                }

                final ByteIterable timestamp = result.subIterable(0, 8);
                final long stored = LongBinding.entryToLong(timestamp);

                if(period != null && period.getPeriod() > 0) {
                    final long expiration = stored + period.getUnit().toMillis(period.getPeriod());
                    if(System.currentTimeMillis() >= expiration) {
                        store.delete(transaction, IntegerBinding.intToEntry(key));
                        return null;
                    }
                }

                final ByteIterable data = result.subIterable(8, result.getLength() - 8);
                results.add(data);
            }
        } finally {
            transaction.commit();
        }

        final Iterator<ByteIterable> iterator = results.iterator();
        return CloseableIterators.from(new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return CoreData.fromBytes(clazz, iterator.next().getBytesUnsafe());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Get(Champion.class)
    public Champion getChampion(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        final String key = (String)query.get("key");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name", key, "key");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Champion.class, UniqueKeys.forChampionDataQuery(query));
    }

    @Get(ChampionMasteries.class)
    public ChampionMasteries getChampionMasteries(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(ChampionMasteries.class, UniqueKeys.forChampionMasteriesDataQuery(query));
    }

    @Get(ChampionMastery.class)
    public ChampionMastery getChampionMastery(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        final Number championId = (Number)query.get("championId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId", championId, "championId");

        return get(ChampionMastery.class, UniqueKeys.forChampionMasteryDataQuery(query));
    }

    @Get(ChampionMasteryScore.class)
    public ChampionMasteryScore getChampionMasteryScore(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(ChampionMasteryScore.class, UniqueKeys.forChampionMasteryScoreDataQuery(query));
    }

    @Get(ChampionRotation.class)
    public ChampionRotation getChampionRotation(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(ChampionRotation.class, UniqueKeys.forChampionRotationDataQuery(query));
    }

    @Get(Champions.class)
    public Champions getChampions(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Champions.class, UniqueKeys.forChampionsDataQuery(query));
    }

    @Get(CurrentMatch.class)
    public CurrentMatch getCurrentMatch(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(CurrentMatch.class, UniqueKeys.forCurrentMatchDataQuery(query));
    }

    @Get(FeaturedMatches.class)
    public FeaturedMatches getFeaturedMatches(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(FeaturedMatches.class, UniqueKeys.forFeaturedMatchesDataQuery(query));
    }

    @Get(Item.class)
    public Item getItem(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Item.class, UniqueKeys.forItemDataQuery(query));
    }

    @Get(Items.class)
    public Items getItems(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Items.class, UniqueKeys.forItemsDataQuery(query));
    }

    @Get(Languages.class)
    public Languages getLanguages(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(Languages.class, UniqueKeys.forLanguagesDataQuery(query));
    }

    @Get(LanguageStrings.class)
    public LanguageStrings getLanguageStrings(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }
        }

        return get(LanguageStrings.class, UniqueKeys.forLanguageStringsDataQuery(query));
    }

    @Get(League.class)
    public League getLeague(final java.util.Map<String, Object> query, final PipelineContext context) {
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

        return get(League.class, UniqueKeys.forLeagueDataQuery(query));
    }

    @Get(LeaguePositions.class)
    public LeaguePositions getLeaguePositions(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(LeaguePositions.class, UniqueKeys.forLeaguePositionsDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Champion.class)
    public CloseableIterator<Champion> getManyChampion(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        final Iterable<String> keys = (Iterable<String>)query.get("keys");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names", keys, "keys");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Champion.class, UniqueKeys.forManyChampionDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMasteries.class)
    public CloseableIterator<ChampionMasteries> getManyChampionMasteries(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(ChampionMasteries.class, UniqueKeys.forManyChampionMasteriesDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMastery.class)
    public CloseableIterator<ChampionMastery> getManyChampionMastery(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        final Iterable<Number> iter = (Iterable<Number>)query.get("championIds");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId", iter, "championIds");

        return get(ChampionMastery.class, UniqueKeys.forManyChampionMasteryDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMasteryScore.class)
    public CloseableIterator<ChampionMasteryScore> getManyChampionMasteryScore(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(ChampionMasteryScore.class, UniqueKeys.forManyChampionMasteryScoreDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionRotation.class)
    public CloseableIterator<ChampionRotation> getManyChampionRotation(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(ChampionRotation.class, UniqueKeys.forManyChampionRotationDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Champions.class)
    public CloseableIterator<Champions> getManyChampions(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.keySet().containsAll(Lists.newArrayList("locale", "includedData", "dataById"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }

            if(!query.containsKey("dataById")) {
                query.put("dataById", Boolean.FALSE);
            }
        }

        return get(Champions.class, UniqueKeys.forManyChampionsDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(CurrentMatch.class)
    public CloseableIterator<CurrentMatch> getManyCurrentMatch(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(CurrentMatch.class, UniqueKeys.forManyCurrentMatchDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(FeaturedMatches.class)
    public CloseableIterator<FeaturedMatches> getManyFeaturedMatches(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(FeaturedMatches.class, UniqueKeys.forManyFeaturedMatchesDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Item.class)
    public CloseableIterator<Item> getManyItem(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Item.class, UniqueKeys.forManyItemDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Items.class)
    public CloseableIterator<Items> getManyItems(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.keySet().containsAll(Lists.newArrayList("locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Items.class, UniqueKeys.forManyItemsDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Languages.class)
    public CloseableIterator<Languages> getManyLanguages(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(Languages.class, UniqueKeys.forManyLanguagesDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(LanguageStrings.class)
    public CloseableIterator<LanguageStrings> getManyLanguageStrings(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("locales");
        Utilities.checkNotNull(platform, "platform", iter, "locales");

        if(!query.containsKey("version")) {
            query = new HashMap<>(query);
            query.put("version", getCurrentVersion(platform, context));
        }

        return get(LanguageStrings.class, UniqueKeys.forManyLanguageStringsDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(League.class)
    public CloseableIterator<League> getManyLeague(final java.util.Map<String, Object> query, final PipelineContext context) {
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

        return get(League.class, UniqueKeys.forManyLeagueDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(LeaguePositions.class)
    public CloseableIterator<LeaguePositions> getManyLeaguePositions(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(LeaguePositions.class, UniqueKeys.forManyLeaguePositionsDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Map.class)
    public CloseableIterator<Map> getManyMap(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }
        }

        return get(Map.class, UniqueKeys.forManyMapDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Maps.class)
    public CloseableIterator<Maps> getManyMaps(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.containsKey("locale")) {
            query = new HashMap<>(query);
            query.put("locale", platform.getDefaultLocale());
        }

        return get(Maps.class, UniqueKeys.forManyMapsDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Masteries.class)
    public CloseableIterator<Masteries> getManyMasteries(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.keySet().containsAll(Lists.newArrayList("locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Masteries.class, UniqueKeys.forManyMasteriesDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Mastery.class)
    public CloseableIterator<Mastery> getManyMastery(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Mastery.class, UniqueKeys.forManyMasteryDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Match.class)
    public CloseableIterator<Match> getManyMatch(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("matchIds");
        Utilities.checkNotNull(platform, "platform", iter, "matchIds");

        return get(Match.class, UniqueKeys.forManyMatchDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Patch.class)
    public CloseableIterator<Patch> getManyPatch(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkNotNull(platform, "platform", names, "names");

        return get(Patch.class, UniqueKeys.forManyPatchDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Patches.class)
    public CloseableIterator<Patches> getManyPatches(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(Patches.class, UniqueKeys.forManyPatchesDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ProfileIcon.class)
    public CloseableIterator<ProfileIcon> getManyProfileIcon(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        Utilities.checkNotNull(platform, "platform", ids, "ids");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }
        }

        return get(ProfileIcon.class, UniqueKeys.forManyProfileIconDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ProfileIcons.class)
    public CloseableIterator<ProfileIcons> getManyProfileIcons(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.containsKey("locale")) {
            query = new HashMap<>(query);
            query.put("locale", platform.getDefaultLocale());
        }

        return get(ProfileIcons.class, UniqueKeys.forManyProfileIconsDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Realm.class)
    public CloseableIterator<Realm> getManyRealm(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(Realm.class, UniqueKeys.forManyRealmDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ReforgedRune.class)
    public CloseableIterator<ReforgedRune> getManyReforgedRune(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        final Iterable<String> keys = (Iterable<String>)query.get("keys");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names", keys, "keys");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }
        }

        return get(ReforgedRune.class, UniqueKeys.forManyReforgedRuneDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ReforgedRunes.class)
    public CloseableIterator<ReforgedRunes> getManyReforgedRunes(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.containsKey("locale")) {
            query = new HashMap<>(query);
            query.put("locale", platform.getDefaultLocale());
        }

        return get(ReforgedRunes.class, UniqueKeys.forManyReforgedRunesDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Rune.class)
    public CloseableIterator<Rune> getManyRune(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Rune.class, UniqueKeys.forManyRuneDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Runes.class)
    public CloseableIterator<Runes> getManyRunes(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.keySet().containsAll(Lists.newArrayList("locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Runes.class, UniqueKeys.forManyRunesDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ShardStatus.class)
    public CloseableIterator<ShardStatus> getManyShardStatus(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(ShardStatus.class, UniqueKeys.forManyShardStatusDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Summoner.class)
    public CloseableIterator<Summoner> getManySummoner(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<String> puuids = (Iterable<String>)query.get("puuids");
        final Iterable<String> accountIds = (Iterable<String>)query.get("accountIds");
        final Iterable<String> summonerIds = (Iterable<String>)query.get("ids");
        final Iterable<String> summonerNames = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(puuids, "puuids", accountIds, "accountIds", summonerIds, "ids", summonerNames, "names");

        return get(Summoner.class, UniqueKeys.forManySummonerDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerSpell.class)
    public CloseableIterator<SummonerSpell> getManySummonerSpell(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> ids = (Iterable<Number>)query.get("ids");
        final Iterable<String> names = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(ids, "ids", names, "names");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(SummonerSpell.class, UniqueKeys.forManySummonerSpellDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerSpells.class)
    public CloseableIterator<SummonerSpells> getManySummonerSpells(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.keySet().containsAll(Lists.newArrayList("locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(SummonerSpells.class, UniqueKeys.forManySummonerSpellsDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Timeline.class)
    public CloseableIterator<Timeline> getManyTimeline(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("matchIds");
        Utilities.checkNotNull(platform, "platform", iter, "matchIds");

        return get(Timeline.class, UniqueKeys.forManyTimelineDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(TournamentMatches.class)
    public CloseableIterator<TournamentMatches> getManyTournamentMatches(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("tournamentCodes");
        Utilities.checkNotNull(platform, "platform", iter, "tournamentCodes");

        return get(TournamentMatches.class, UniqueKeys.forManyTournamentMatchesDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(VerificationString.class)
    public CloseableIterator<VerificationString> getManyVerificationString(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(VerificationString.class, UniqueKeys.forManyVerificationStringDataQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Versions.class)
    public CloseableIterator<Versions> getManyVersions(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(Versions.class, UniqueKeys.forManyVersionsDataQuery(query));
    }

    @Get(Map.class)
    public Map getMap(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }
        }

        return get(Map.class, UniqueKeys.forMapDataQuery(query));
    }

    @Get(Maps.class)
    public Maps getMaps(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }
        }

        return get(Maps.class, UniqueKeys.forMapsDataQuery(query));
    }

    @Get(Masteries.class)
    public Masteries getMasteries(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Masteries.class, UniqueKeys.forMasteriesDataQuery(query));
    }

    @Get(Mastery.class)
    public Mastery getMastery(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Mastery.class, UniqueKeys.forMasteryDataQuery(query));
    }

    @Get(Match.class)
    public Match getMatch(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number matchId = (Number)query.get("matchId");
        Utilities.checkNotNull(platform, "platform", matchId, "matchId");

        return get(Match.class, UniqueKeys.forMatchDataQuery(query));
    }

    @Get(Patch.class)
    public Patch getPatch(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final String name = (String)query.get("name");
        Utilities.checkNotNull(platform, "platform", name, "name");

        return get(Patch.class, UniqueKeys.forPatchDataQuery(query));
    }

    @Get(Patches.class)
    public Patches getPatches(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(Patches.class, UniqueKeys.forPatchesDataQuery(query));
    }

    @Get(ProfileIcon.class)
    public ProfileIcon getProfileIcon(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        Utilities.checkAtLeastOneNotNull(id, "id");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }
        }

        return get(ProfileIcon.class, UniqueKeys.forProfileIconDataQuery(query));
    }

    @Get(ProfileIcons.class)
    public ProfileIcons getProfileIcons(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }
        }

        return get(ProfileIcons.class, UniqueKeys.forProfileIconsDataQuery(query));
    }

    @Get(Realm.class)
    public Realm getRealm(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(Realm.class, UniqueKeys.forRealmDataQuery(query));
    }

    @Get(ReforgedRune.class)
    public ReforgedRune getReforgedRune(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        final String key = (String)query.get("key");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name", key, "key");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }
        }

        return get(ReforgedRune.class, UniqueKeys.forReforgedRuneDataQuery(query));
    }

    @Get(ReforgedRunes.class)
    public ReforgedRunes getReforgedRunes(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }
        }

        return get(ReforgedRunes.class, UniqueKeys.forReforgedRunesDataQuery(query));
    }

    @Get(Rune.class)
    public Rune getRune(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Rune.class, UniqueKeys.forRuneDataQuery(query));
    }

    @Get(Runes.class)
    public Runes getRunes(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(Runes.class, UniqueKeys.forRunesDataQuery(query));
    }

    @Get(ShardStatus.class)
    public ShardStatus getShardStatus(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(ShardStatus.class, UniqueKeys.forShardStatusDataQuery(query));
    }

    @Get(Summoner.class)
    public Summoner getSummoner(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final String puuid = (String)query.get("puuid");
        final String accountId = (String)query.get("accountId");
        final String summonerId = (String)query.get("id");
        final String summonerName = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(puuid, "puuid", accountId, "accountId", summonerId, "id", summonerName, "name");

        return get(Summoner.class, UniqueKeys.forSummonerDataQuery(query));
    }

    @Get(SummonerSpell.class)
    public SummonerSpell getSummonerSpell(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number id = (Number)query.get("id");
        final String name = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(id, "id", name, "name");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(SummonerSpell.class, UniqueKeys.forSummonerSpellDataQuery(query));
    }

    @Get(SummonerSpells.class)
    public SummonerSpells getSummonerSpells(java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData"))) {
            query = new HashMap<>(query);

            if(!query.containsKey("version")) {
                query.put("version", getCurrentVersion(platform, context));
            }

            if(!query.containsKey("locale")) {
                query.put("locale", platform.getDefaultLocale());
            }

            if(!query.containsKey("includedData")) {
                query.put("includedData", Collections.<String> emptySet());
            }
        }

        return get(SummonerSpells.class, UniqueKeys.forSummonerSpellsDataQuery(query));
    }

    @Get(Timeline.class)
    public Timeline getTimeline(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number matchId = (Number)query.get("matchId");
        Utilities.checkNotNull(platform, "platform", matchId, "matchId");

        return get(Timeline.class, UniqueKeys.forTimelineDataQuery(query));
    }

    @Get(TournamentMatches.class)
    public TournamentMatches getTournamentMatches(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final String tournamentCode = (String)query.get("tournamentCode");
        Utilities.checkNotNull(platform, "platform", tournamentCode, "tournamentCode");

        return get(TournamentMatches.class, UniqueKeys.forTournamentMatchesDataQuery(query));
    }

    @Get(VerificationString.class)
    public VerificationString getVerificationString(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(VerificationString.class, UniqueKeys.forVerificationStringDataQuery(query));
    }

    @Get(Versions.class)
    public Versions getVersions(final java.util.Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(Versions.class, UniqueKeys.forVersionsDataQuery(query));
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

    public void prune() {
        final Transaction transaction = xodus.beginTransaction();
        try {
            for(final String className : expirationPeriods.keySet()) {
                final ExpirationPeriod period = expirationPeriods.get(className);
                if(period == null || period.getPeriod() <= 0) {
                    continue;
                }

                final long millis = period.getUnit().toMillis(period.getPeriod());

                final Store store = xodus.openStore(className, STORE_CONFIG, transaction);
                try(final Cursor cursor = store.openCursor(transaction)) {
                    while(cursor.getNext()) {
                        final ByteIterable value = cursor.getValue();
                        final ByteIterable timestamp = value.subIterable(0, 8);
                        final long stored = LongBinding.entryToLong(timestamp);

                        if(System.currentTimeMillis() >= stored + millis) {
                            cursor.deleteCurrent();
                        }
                    }
                }
            }
        } finally {
            transaction.commit();
        }
    }

    public void prune(final Class<?> clazz) {
        final ExpirationPeriod period = expirationPeriods.get(clazz.getCanonicalName());
        if(period == null || period.getPeriod() <= 0) {
            return;
        }

        final long millis = period.getUnit().toMillis(period.getPeriod());

        final Transaction transaction = xodus.beginTransaction();
        try {
            final Store store = xodus.openStore(clazz.getCanonicalName(), STORE_CONFIG, transaction);
            try(final Cursor cursor = store.openCursor(transaction)) {
                while(cursor.getNext()) {
                    final ByteIterable value = cursor.getValue();
                    final ByteIterable timestamp = value.subIterable(0, 8);
                    final long stored = LongBinding.entryToLong(timestamp);

                    if(System.currentTimeMillis() >= stored + millis) {
                        cursor.deleteCurrent();
                    }
                }
            }
        } finally {
            transaction.commit();
        }
    }

    private <T extends CoreData> void put(final Class<T> clazz, final int key, final T value) {
        final Transaction transaction = xodus.beginTransaction();
        try {
            final Store store = xodus.openStore(clazz.getCanonicalName(), STORE_CONFIG, transaction);
            store.put(transaction, IntegerBinding.intToEntry(key), toByteIterable(value));
        } finally {
            transaction.commit();
        }
    }

    private <T extends CoreData> void put(final Class<T> clazz, final Iterable<Integer> keys, final Iterable<T> values) {
        final Transaction transaction = xodus.beginTransaction();
        try {
            final Store store = xodus.openStore(clazz.getCanonicalName(), STORE_CONFIG, transaction);
            final Iterator<Integer> keyIterator = keys.iterator();
            final Iterator<T> valueIterator = values.iterator();

            while(keyIterator.hasNext() && valueIterator.hasNext()) {
                store.put(transaction, IntegerBinding.intToEntry(keyIterator.next()), toByteIterable(valueIterator.next()));
            }
        } finally {
            transaction.commit();
        }
    }

    @Put(Champion.class)
    public void putChampion(final Champion champion, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Champion> values = new ArrayList<>();
        for(final int key : UniqueKeys.forChampionData(champion)) {
            keys.add(key);
            values.add(champion);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Champion.class, keys, values);
    }

    @Put(ChampionMasteries.class)
    public void putChampionMasteries(final ChampionMasteries masteries, final PipelineContext context) {
        put(ChampionMasteries.class, UniqueKeys.forChampionMasteriesData(masteries), masteries);
        putManyChampionMastery(masteries, context);
    }

    @Put(ChampionMastery.class)
    public void putChampionMastery(final ChampionMastery mastery, final PipelineContext context) {
        put(ChampionMastery.class, UniqueKeys.forChampionMasteryData(mastery), mastery);
    }

    @Put(ChampionMasteryScore.class)
    public void putChampionMasteryScore(final ChampionMasteryScore score, final PipelineContext context) {
        put(ChampionMasteryScore.class, UniqueKeys.forChampionMasteryScoreData(score), score);
    }

    @Put(ChampionRotation.class)
    public void putChampionRotation(final ChampionRotation rotation, final PipelineContext context) {
        put(ChampionRotation.class, UniqueKeys.forChampionRotationData(rotation), rotation);
    }

    @Put(Champions.class)
    public void putChampions(final Champions list, final PipelineContext context) {
        put(Champions.class, UniqueKeys.forChampionsData(list), list);
        putManyChampion(list, context);
    }

    @Put(CurrentMatch.class)
    public void putCurrentMatch(final CurrentMatch game, final PipelineContext context) {
        put(CurrentMatch.class, UniqueKeys.forCurrentMatchData(game), game);
    }

    @Put(FeaturedMatches.class)
    public void putFeaturedMatches(final FeaturedMatches games, final PipelineContext context) {
        put(FeaturedMatches.class, UniqueKeys.forFeaturedMatchesData(games), games);
    }

    @Put(Item.class)
    public void putItem(final Item item, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Item> values = new ArrayList<>();
        for(final int key : UniqueKeys.forItemData(item)) {
            keys.add(key);
            values.add(item);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Item.class, keys, values);
    }

    @Put(Items.class)
    public void putItems(final Items list, final PipelineContext context) {
        put(Items.class, UniqueKeys.forItemsData(list), list);
        putManyItem(list, context);
    }

    @Put(Languages.class)
    public void putLanguages(final Languages languages, final PipelineContext context) {
        put(Languages.class, UniqueKeys.forLanguagesData(languages), languages);
    }

    @Put(LanguageStrings.class)
    public void putLanguageStrings(final LanguageStrings strings, final PipelineContext context) {
        put(LanguageStrings.class, UniqueKeys.forLanguageStringsData(strings), strings);
    }

    @Put(League.class)
    public void putLeague(final League list, final PipelineContext context) {
        final int[] keys = UniqueKeys.forLeagueData(list);
        final List<Integer> keyList = new ArrayList<>(keys.length);
        final List<League> valueList = new ArrayList<>(keys.length);
        for(final int key : keys) {
            keyList.add(key);
            valueList.add(list);
        }

        put(League.class, keyList, valueList);
    }

    @Put(LeaguePositions.class)
    public void putLeaguePositions(final LeaguePositions positions, final PipelineContext context) {
        put(LeaguePositions.class, UniqueKeys.forLeaguePositionsData(positions), positions);
    }

    @PutMany(Summoner.class)
    public void putManSummoner(final Iterable<Summoner> summoners, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Summoner> values = new ArrayList<>();
        for(final Summoner summoner : summoners) {
            for(final int key : UniqueKeys.forSummonerData(summoner)) {
                keys.add(key);
                values.add(summoner);
            }
        }
        keys.trimToSize();
        values.trimToSize();

        put(Summoner.class, keys, values);
    }

    @PutMany(Champion.class)
    public void putManyChampion(final Iterable<Champion> champions, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Champion> values = new ArrayList<>();
        for(final Champion champion : champions) {
            for(final int key : UniqueKeys.forChampionData(champion)) {
                keys.add(key);
                values.add(champion);
            }
        }
        keys.trimToSize();
        values.trimToSize();

        put(Champion.class, keys, values);
    }

    @PutMany(ChampionMasteries.class)
    public void putManyChampionMasteries(final Iterable<ChampionMasteries> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ChampionMasteries list : lists) {
            keys.add(UniqueKeys.forChampionMasteriesData(list));
        }
        keys.trimToSize();

        put(ChampionMasteries.class, keys, lists);
        putManyChampionMastery(Iterables.concat(lists), context);
    }

    @PutMany(ChampionMastery.class)
    public void putManyChampionMastery(final Iterable<ChampionMastery> masteries, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ChampionMastery mastery : masteries) {
            keys.add(UniqueKeys.forChampionMasteryData(mastery));
        }
        keys.trimToSize();

        put(ChampionMastery.class, keys, masteries);
    }

    @PutMany(ChampionMasteryScore.class)
    public void putManyChampionMasteryScore(final Iterable<ChampionMasteryScore> scores, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ChampionMasteryScore score : scores) {
            keys.add(UniqueKeys.forChampionMasteryScoreData(score));
        }
        keys.trimToSize();

        put(ChampionMasteryScore.class, keys, scores);
    }

    @PutMany(ChampionRotation.class)
    public void putManyChampionRotation(final Iterable<ChampionRotation> rotations, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ChampionRotation rotation : rotations) {
            keys.add(UniqueKeys.forChampionRotationData(rotation));
        }
        keys.trimToSize();

        put(ChampionRotation.class, keys, rotations);
    }

    @PutMany(Champions.class)
    public void putManyChampions(final Iterable<Champions> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Champions list : lists) {
            keys.add(UniqueKeys.forChampionsData(list));
        }
        keys.trimToSize();

        put(Champions.class, keys, lists);
        putManyChampion(Iterables.concat(lists), context);
    }

    @PutMany(CurrentMatch.class)
    public void putManyCurrentMatch(final Iterable<CurrentMatch> games, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final CurrentMatch game : games) {
            keys.add(UniqueKeys.forCurrentMatchData(game));
        }
        keys.trimToSize();

        put(CurrentMatch.class, keys, games);
    }

    @PutMany(FeaturedMatches.class)
    public void putManyFeaturedMatches(final Iterable<FeaturedMatches> games, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final FeaturedMatches game : games) {
            keys.add(UniqueKeys.forFeaturedMatchesData(game));
        }
        keys.trimToSize();

        put(FeaturedMatches.class, keys, games);
    }

    @PutMany(Item.class)
    public void putManyItem(final Iterable<Item> items, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Item> values = new ArrayList<>();
        for(final Item item : items) {
            for(final int key : UniqueKeys.forItemData(item)) {
                keys.add(key);
                values.add(item);
            }
        }
        keys.trimToSize();
        values.trimToSize();

        put(Item.class, keys, values);
    }

    @PutMany(Items.class)
    public void putManyItems(final Iterable<Items> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Items list : lists) {
            keys.add(UniqueKeys.forItemsData(list));
        }
        keys.trimToSize();

        put(Items.class, keys, lists);
        putManyItem(Iterables.concat(lists), context);
    }

    @PutMany(Languages.class)
    public void putManyLanguages(final Iterable<Languages> languages, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Languages language : languages) {
            keys.add(UniqueKeys.forLanguagesData(language));
        }
        keys.trimToSize();

        put(Languages.class, keys, languages);
    }

    @PutMany(LanguageStrings.class)
    public void putManyLanguageStrings(final Iterable<LanguageStrings> strings, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final LanguageStrings string : strings) {
            keys.add(UniqueKeys.forLanguageStringsData(string));
        }
        keys.trimToSize();

        put(LanguageStrings.class, keys, strings);
    }

    @PutMany(League.class)
    public void putManyLeague(final Iterable<League> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<League> values = new ArrayList<>();
        for(final League list : lists) {
            for(final int key : UniqueKeys.forLeagueData(list)) {
                keys.add(key);
                values.add(list);
            }
        }
        keys.trimToSize();
        values.trimToSize();

        put(League.class, keys, values);
    }

    @PutMany(LeaguePositions.class)
    public void putManyLeaguePositions(final Iterable<LeaguePositions> positions, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final LeaguePositions position : positions) {
            keys.add(UniqueKeys.forLeaguePositionsData(position));
        }
        keys.trimToSize();

        put(LeaguePositions.class, keys, positions);
    }

    @PutMany(Map.class)
    public void putManyMap(final Iterable<Map> maps, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Map> values = new ArrayList<>();
        for(final Map map : maps) {
            for(final int key : UniqueKeys.forMapData(map)) {
                keys.add(key);
                values.add(map);
            }
        }
        keys.trimToSize();
        values.trimToSize();

        put(Map.class, keys, values);
    }

    @PutMany(Maps.class)
    public void putManyMaps(final Iterable<Maps> maps, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Maps map : maps) {
            keys.add(UniqueKeys.forMapsData(map));
        }
        keys.trimToSize();

        put(Maps.class, keys, maps);
        putManyMap(Iterables.concat(maps), context);
    }

    @PutMany(Masteries.class)
    public void putManyMasteries(final Iterable<Masteries> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Masteries list : lists) {
            keys.add(UniqueKeys.forMasteriesData(list));
        }
        keys.trimToSize();

        put(Masteries.class, keys, lists);
        putManyMastery(Iterables.concat(lists), context);
    }

    @PutMany(Mastery.class)
    public void putManyMastery(final Iterable<Mastery> items, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Mastery> values = new ArrayList<>();
        for(final Mastery item : items) {
            for(final int key : UniqueKeys.forMasteryData(item)) {
                keys.add(key);
                values.add(item);
            }
        }
        keys.trimToSize();
        values.trimToSize();

        put(Mastery.class, keys, values);
    }

    @PutMany(Match.class)
    public void putManyMatch(final Iterable<Match> matches, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Match match : matches) {
            keys.add(UniqueKeys.forMatchData(match));
        }
        keys.trimToSize();

        put(Match.class, keys, matches);
    }

    @PutMany(Patch.class)
    public void putManyPatch(final Iterable<Patch> patches, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Patch> values = new ArrayList<>();
        for(final Patch patch : patches) {
            keys.add(UniqueKeys.forPatchData(patch));
            values.add(patch);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Patch.class, keys, values);
    }

    @PutMany(Patches.class)
    public void putManyPatches(final Iterable<Patches> patches, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Patches patch : patches) {
            keys.add(UniqueKeys.forPatchesData(patch));
        }
        keys.trimToSize();

        put(Patches.class, keys, patches);
        final List<List<Patch>> toStore = new ArrayList<>(keys.size());
        for(final Patches patch : patches) {
            toStore.add(new ArrayList<>(patch));
        }
        putManyPatch(Iterables.concat(toStore), context);
    }

    @PutMany(ProfileIcon.class)
    public void putManyProfileIcon(final Iterable<ProfileIcon> icons, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ProfileIcon icon : icons) {
            keys.add(UniqueKeys.forProfileIconData(icon));
        }
        keys.trimToSize();

        put(ProfileIcon.class, keys, icons);
    }

    @PutMany(ProfileIcons.class)
    public void putManyProfileIcons(final Iterable<ProfileIcons> icons, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ProfileIcons icon : icons) {
            keys.add(UniqueKeys.forProfileIconsData(icon));
        }
        keys.trimToSize();

        put(ProfileIcons.class, keys, icons);
        putManyProfileIcon(Iterables.concat(icons), context);
    }

    @PutMany(Realm.class)
    public void putManyRealm(final Iterable<Realm> realms, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Realm realm : realms) {
            keys.add(UniqueKeys.forRealmData(realm));
        }
        keys.trimToSize();

        put(Realm.class, keys, realms);
    }

    @PutMany(ReforgedRune.class)
    public void putManyReforgedRune(final Iterable<ReforgedRune> runes, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<ReforgedRune> values = new ArrayList<>();
        for(final ReforgedRune rune : runes) {
            for(final int key : UniqueKeys.forReforgedRuneData(rune)) {
                keys.add(key);
                values.add(rune);
            }
        }
        keys.trimToSize();
        values.trimToSize();

        put(ReforgedRune.class, keys, values);
    }

    @PutMany(ReforgedRunes.class)
    public void putManyReforgedRunes(final Iterable<ReforgedRunes> runes, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ReforgedRunes rune : runes) {
            keys.add(UniqueKeys.forReforgedRunesData(rune));
        }
        keys.trimToSize();

        put(ReforgedRunes.class, keys, runes);
        putManyReforgedRune(Iterables.concat(runes), context);
    }

    @PutMany(Rune.class)
    public void putManyRune(final Iterable<Rune> runes, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Rune> values = new ArrayList<>();
        for(final Rune rune : runes) {
            for(final int key : UniqueKeys.forRuneData(rune)) {
                keys.add(key);
                values.add(rune);
            }
        }
        keys.trimToSize();
        values.trimToSize();

        put(Rune.class, keys, values);
    }

    @PutMany(Runes.class)
    public void putManyRunes(final Iterable<Runes> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Runes list : lists) {
            keys.add(UniqueKeys.forRunesData(list));
        }
        keys.trimToSize();

        put(Runes.class, keys, lists);
        putManyRune(Iterables.concat(lists), context);
    }

    @PutMany(ShardStatus.class)
    public void putManyShardStatus(final Iterable<ShardStatus> statuses, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ShardStatus status : statuses) {
            keys.add(UniqueKeys.forShardStatusData(status));
        }
        keys.trimToSize();

        put(ShardStatus.class, keys, statuses);
    }

    @PutMany(SummonerSpell.class)
    public void putManySummonerSpell(final Iterable<SummonerSpell> summonerSpells, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<SummonerSpell> values = new ArrayList<>();
        for(final SummonerSpell summonerSpell : summonerSpells) {
            for(final int key : UniqueKeys.forSummonerSpellData(summonerSpell)) {
                keys.add(key);
                values.add(summonerSpell);
            }
        }
        keys.trimToSize();
        values.trimToSize();

        put(SummonerSpell.class, keys, values);
    }

    @PutMany(SummonerSpells.class)
    public void putManySummonerSpells(final Iterable<SummonerSpells> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final SummonerSpells list : lists) {
            keys.add(UniqueKeys.forSummonerSpellsData(list));
        }
        keys.trimToSize();

        put(SummonerSpells.class, keys, lists);
        putManySummonerSpell(Iterables.concat(lists), context);
    }

    @PutMany(Timeline.class)
    public void putManyTimeline(final Iterable<Timeline> timelines, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Timeline timeline : timelines) {
            keys.add(UniqueKeys.forTimelineData(timeline));
        }
        keys.trimToSize();

        put(Timeline.class, keys, timelines);
    }

    @PutMany(TournamentMatches.class)
    public void putManyTournamentMatches(final Iterable<TournamentMatches> matches, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final TournamentMatches match : matches) {
            keys.add(UniqueKeys.forTournamentMatchesData(match));
        }
        keys.trimToSize();

        put(TournamentMatches.class, keys, matches);
    }

    @PutMany(VerificationString.class)
    public void putManyVerificationString(final Iterable<VerificationString> strings, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final VerificationString string : strings) {
            keys.add(UniqueKeys.forVerificationStringData(string));
        }
        keys.trimToSize();

        put(VerificationString.class, keys, strings);
    }

    @PutMany(Versions.class)
    public void putManyVersions(final Iterable<Versions> versions, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Versions version : versions) {
            keys.add(UniqueKeys.forVersionsData(version));
        }
        keys.trimToSize();

        put(Versions.class, keys, versions);
    }

    @Put(Map.class)
    public void putMap(final Map map, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Map> values = new ArrayList<>();
        for(final int key : UniqueKeys.forMapData(map)) {
            keys.add(key);
            values.add(map);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Map.class, keys, values);
    }

    @Put(Maps.class)
    public void putMaps(final Maps data, final PipelineContext context) {
        put(Maps.class, UniqueKeys.forMapsData(data), data);
        putManyMap(data, context);
    }

    @Put(Masteries.class)
    public void putMasteries(final Masteries list, final PipelineContext context) {
        put(Masteries.class, UniqueKeys.forMasteriesData(list), list);
        putManyMastery(list, context);
    }

    @Put(Mastery.class)
    public void putMastery(final Mastery mastery, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Mastery> values = new ArrayList<>();
        for(final int key : UniqueKeys.forMasteryData(mastery)) {
            keys.add(key);
            values.add(mastery);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Mastery.class, keys, values);
    }

    @Put(Match.class)
    public void putMatch(final Match match, final PipelineContext context) {
        put(Match.class, UniqueKeys.forMatchData(match), match);
    }

    @Put(Patch.class)
    public void putPatch(final Patch patch, final PipelineContext context) {
        put(Patch.class, UniqueKeys.forPatchData(patch), patch);
    }

    @Put(Patches.class)
    public void putPatches(final Patches patches, final PipelineContext context) {
        put(Patches.class, UniqueKeys.forPatchesData(patches), patches);
        putManyPatch(patches, context);
    }

    @Put(ProfileIcon.class)
    public void putProfileIcon(final ProfileIcon icon, final PipelineContext context) {
        put(ProfileIcon.class, UniqueKeys.forProfileIconData(icon), icon);
    }

    @Put(ProfileIcons.class)
    public void putProfileIcons(final ProfileIcons data, final PipelineContext context) {
        put(ProfileIcons.class, UniqueKeys.forProfileIconsData(data), data);
        putManyProfileIcon(data, context);
    }

    @Put(Realm.class)
    public void putRealm(final Realm realm, final PipelineContext context) {
        put(Realm.class, UniqueKeys.forRealmData(realm), realm);
    }

    @Put(ReforgedRune.class)
    public void putReforgedRune(final ReforgedRune rune, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<ReforgedRune> values = new ArrayList<>();
        for(final int key : UniqueKeys.forReforgedRuneData(rune)) {
            keys.add(key);
            values.add(rune);
        }
        keys.trimToSize();
        values.trimToSize();

        put(ReforgedRune.class, keys, values);
    }

    @Put(ReforgedRunes.class)
    public void putReforgedRunes(final ReforgedRunes runes, final PipelineContext context) {
        put(ReforgedRunes.class, UniqueKeys.forReforgedRunesData(runes), runes);
        putManyReforgedRune(runes, context);
    }

    @Put(Rune.class)
    public void putRune(final Rune rune, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Rune> values = new ArrayList<>();
        for(final int key : UniqueKeys.forRuneData(rune)) {
            keys.add(key);
            values.add(rune);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Rune.class, keys, values);
    }

    @Put(Runes.class)
    public void putRunes(final Runes list, final PipelineContext context) {
        put(Runes.class, UniqueKeys.forRunesData(list), list);
        putManyRune(list, context);
    }

    @Put(ShardStatus.class)
    public void putShardStatus(final ShardStatus status, final PipelineContext context) {
        put(ShardStatus.class, UniqueKeys.forShardStatusData(status), status);
    }

    @Put(Summoner.class)
    public void putSummoner(final Summoner summoner, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Summoner> values = new ArrayList<>();
        for(final int key : UniqueKeys.forSummonerData(summoner)) {
            keys.add(key);
            values.add(summoner);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Summoner.class, keys, values);
    }

    @Put(SummonerSpell.class)
    public void putSummonerSpell(final SummonerSpell summonerSpell, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<SummonerSpell> values = new ArrayList<>();
        for(final int key : UniqueKeys.forSummonerSpellData(summonerSpell)) {
            keys.add(key);
            values.add(summonerSpell);
        }
        keys.trimToSize();
        values.trimToSize();

        put(SummonerSpell.class, keys, values);
    }

    @Put(SummonerSpells.class)
    public void putSummonerSpells(final SummonerSpells list, final PipelineContext context) {
        put(SummonerSpells.class, UniqueKeys.forSummonerSpellsData(list), list);
        putManySummonerSpell(list, context);
    }

    @Put(Timeline.class)
    public void putTimeline(final Timeline timeline, final PipelineContext context) {
        put(Timeline.class, UniqueKeys.forTimelineData(timeline), timeline);
    }

    @Put(TournamentMatches.class)
    public void putTournamentMatches(final TournamentMatches matches, final PipelineContext context) {
        put(TournamentMatches.class, UniqueKeys.forTournamentMatchesData(matches), matches);
    }

    @Put(VerificationString.class)
    public void putVerificationString(final VerificationString string, final PipelineContext context) {
        put(VerificationString.class, UniqueKeys.forVerificationStringData(string), string);
    }

    @Put(Versions.class)
    public void putVersions(final Versions versions, final PipelineContext context) {
        put(Versions.class, UniqueKeys.forVersionsData(versions), versions);
    }
}

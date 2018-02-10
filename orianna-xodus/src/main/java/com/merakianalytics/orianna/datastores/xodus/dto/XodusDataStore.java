package com.merakianalytics.orianna.datastores.xodus.dto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import com.merakianalytics.orianna.types.dto.DataObject;
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
import com.merakianalytics.orianna.types.dto.staticdata.MapDetails;
import com.merakianalytics.orianna.types.dto.staticdata.Mastery;
import com.merakianalytics.orianna.types.dto.staticdata.MasteryList;
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
        private static final Map<String, ExpirationPeriod> DEFAULT_EXPIRATION_PERIODS = ImmutableMap.<String, ExpirationPeriod> builder()
            .put(com.merakianalytics.orianna.types.dto.champion.Champion.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(com.merakianalytics.orianna.types.dto.champion.ChampionList.class.getCanonicalName(), ExpirationPeriod.create(6L, TimeUnit.HOURS))
            .put(ChampionMastery.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(ChampionMasteries.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(ChampionMasteryScore.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
            .put(LeagueList.class.getCanonicalName(), ExpirationPeriod.create(30L, TimeUnit.MINUTES))
            .put(SummonerPositions.class.getCanonicalName(), ExpirationPeriod.create(2L, TimeUnit.HOURS))
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
    private static final Logger LOGGER = LoggerFactory.getLogger(XodusDataStore.class);
    private static final StoreConfig STORE_CONFIG = StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;

    private static String getCurrentVersion(final Platform platform, final PipelineContext context) {
        final Realm realm = context.getPipeline().get(Realm.class, ImmutableMap.<String, Object> of("platform", platform));
        return realm.getV();
    }

    private static <T extends DataObject> ByteIterable toByteIterable(final T value) {
        return new CompoundByteIterable(new ByteIterable[] {LongBinding.longToEntry(System.currentTimeMillis()), new ArrayByteIterable(value.toBytes())}, 2);
    }

    private final Map<String, ExpirationPeriod> expirationPeriods;

    public XodusDataStore() {
        this(new Configuration());
    }

    public XodusDataStore(final Configuration config) {
        super(config);
        expirationPeriods = config.getExpirationPeriods();
    }

    private <T extends DataObject> T get(final Class<T> clazz, final int key) {
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

        return DataObject.fromBytes(clazz, data.getBytesUnsafe());
    }

    private <T extends DataObject> CloseableIterator<T> get(final Class<T> clazz, final Iterator<Integer> keyIterator) {
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
                return DataObject.fromBytes(clazz, iterator.next().getBytesUnsafe());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Get(Champion.class)
    public Champion getChampion(Map<String, Object> query, final PipelineContext context) {
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

        return get(Champion.class, UniqueKeys.forChampionDtoQuery(query));
    }

    @Get(ChampionList.class)
    public ChampionList getChampionList(Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.keySet().containsAll(Lists.newArrayList("version", "locale", "includedData", "dataById"))) {
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

            if(!query.containsKey("queryById")) {
                query.put("queryById", Boolean.FALSE);
            }
        }

        return get(ChampionList.class, UniqueKeys.forChampionListDtoQuery(query));
    }

    @Get(ChampionMasteries.class)
    public ChampionMasteries getChampionMasteries(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(ChampionMasteries.class, UniqueKeys.forChampionMasteriesDtoQuery(query));
    }

    @Get(ChampionMastery.class)
    public ChampionMastery getChampionMastery(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        final Number championId = (Number)query.get("championId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId", championId, "championId");

        return get(ChampionMastery.class, UniqueKeys.forChampionMasteryDtoQuery(query));
    }

    @Get(ChampionMasteryScore.class)
    public ChampionMasteryScore getChampionMasteryScore(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(ChampionMasteryScore.class, UniqueKeys.forChampionMasteryScoreDtoQuery(query));
    }

    @Get(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public com.merakianalytics.orianna.types.dto.champion.Champion getChampionStatus(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number id = (Number)query.get("id");
        Utilities.checkNotNull(platform, "platform", id, "id");

        return get(com.merakianalytics.orianna.types.dto.champion.Champion.class, UniqueKeys.forChampionStatusDtoQuery(query));
    }

    @Get(com.merakianalytics.orianna.types.dto.champion.ChampionList.class)
    public com.merakianalytics.orianna.types.dto.champion.ChampionList getChampionStatusList(Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        if(!query.containsKey("freeToPlay")) {
            query = new HashMap<>(query);
            query.put("freeToPlay", Boolean.FALSE);
        }

        return get(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, UniqueKeys.forChampionStatusListDtoQuery(query));
    }

    @Get(CurrentGameInfo.class)
    public CurrentGameInfo getCurrentGameInfo(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(CurrentGameInfo.class, UniqueKeys.forCurrentGameInfoDtoQuery(query));
    }

    @Get(FeaturedGames.class)
    public FeaturedGames getFeaturedGames(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(FeaturedGames.class, UniqueKeys.forFeaturedGamesDtoQuery(query));
    }

    @Get(Item.class)
    public Item getItem(Map<String, Object> query, final PipelineContext context) {
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

        return get(Item.class, UniqueKeys.forItemDtoQuery(query));
    }

    @Get(ItemList.class)
    public ItemList getItemList(Map<String, Object> query, final PipelineContext context) {
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

        return get(ItemList.class, UniqueKeys.forItemListDtoQuery(query));
    }

    @Get(Languages.class)
    public Languages getLanguages(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(Languages.class, UniqueKeys.forLanguagesDtoQuery(query));
    }

    @Get(LanguageStrings.class)
    public LanguageStrings getLanguageStrings(Map<String, Object> query, final PipelineContext context) {
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

        return get(LanguageStrings.class, UniqueKeys.forLanguageStringsDtoQuery(query));
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

        return get(LeagueList.class, UniqueKeys.forLeagueListDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Champion.class)
    public CloseableIterator<Champion> getManyChampion(Map<String, Object> query, final PipelineContext context) {
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

        return get(Champion.class, UniqueKeys.forManyChampionDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionList.class)
    public CloseableIterator<ChampionList> getManyChampionList(Map<String, Object> query, final PipelineContext context) {
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

        return get(ChampionList.class, UniqueKeys.forManyChampionListDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMasteries.class)
    public CloseableIterator<ChampionMasteries> getManyChampionMasteries(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(ChampionMasteries.class, UniqueKeys.forManyChampionMasteriesDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMastery.class)
    public CloseableIterator<ChampionMastery> getManyChampionMastery(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        final Iterable<Number> iter = (Iterable<Number>)query.get("championIds");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId", iter, "championIds");

        return get(ChampionMastery.class, UniqueKeys.forManyChampionMasteryDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ChampionMasteryScore.class)
    public CloseableIterator<ChampionMasteryScore> getManyChampionMasteryScore(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(ChampionMasteryScore.class, UniqueKeys.forManyChampionMasteryScoreDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public CloseableIterator<com.merakianalytics.orianna.types.dto.champion.Champion> getManyChampionStatus(final Map<String, Object> query,
        final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("ids");
        Utilities.checkNotNull(platform, "platform", iter, "ids");

        return get(com.merakianalytics.orianna.types.dto.champion.Champion.class, UniqueKeys.forManyChampionStatusDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(com.merakianalytics.orianna.types.dto.champion.ChampionList.class)
    public CloseableIterator<com.merakianalytics.orianna.types.dto.champion.ChampionList> getManyChampionStatusList(Map<String, Object> query,
        final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        if(!query.containsKey("freeToPlay")) {
            query = new HashMap<>(query);
            query.put("freeToPlay", Boolean.FALSE);
        }

        return get(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, UniqueKeys.forManyChampionStatusListDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(CurrentGameInfo.class)
    public CloseableIterator<CurrentGameInfo> getManyCurrentGameInfo(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(CurrentGameInfo.class, UniqueKeys.forManyCurrentGameInfoDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(FeaturedGames.class)
    public CloseableIterator<FeaturedGames> getManyFeaturedGames(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(FeaturedGames.class, UniqueKeys.forManyFeaturedGamesDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Item.class)
    public CloseableIterator<Item> getManyItem(Map<String, Object> query, final PipelineContext context) {
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

        return get(Item.class, UniqueKeys.forManyItemDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ItemList.class)
    public CloseableIterator<ItemList> getManyItemList(Map<String, Object> query, final PipelineContext context) {
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

        return get(ItemList.class, UniqueKeys.forManyItemListDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Languages.class)
    public CloseableIterator<Languages> getManyLanguages(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(Languages.class, UniqueKeys.forManyLanguagesDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(LanguageStrings.class)
    public CloseableIterator<LanguageStrings> getManyLanguageStrings(Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("locales");
        Utilities.checkNotNull(platform, "platform", iter, "locales");

        if(!query.containsKey("version")) {
            query = new HashMap<>(query);
            query.put("version", getCurrentVersion(platform, context));
        }

        return get(LanguageStrings.class, UniqueKeys.forManyLanguageStringsDtoQuery(query));
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

        return get(LeagueList.class, UniqueKeys.forManyLeagueListDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(MapData.class)
    public CloseableIterator<MapData> getManyMapData(Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.containsKey("locale")) {
            query = new HashMap<>(query);
            query.put("locale", platform.getDefaultLocale());
        }

        return get(MapData.class, UniqueKeys.forManyMapDataDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(MapDetails.class)
    public CloseableIterator<MapDetails> getManyMapDetails(Map<String, Object> query, final PipelineContext context) {
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

        return get(MapDetails.class, UniqueKeys.forManyMapDetailsDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Mastery.class)
    public CloseableIterator<Mastery> getManyMastery(Map<String, Object> query, final PipelineContext context) {
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

        return get(Mastery.class, UniqueKeys.forManyMasteryDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(MasteryList.class)
    public CloseableIterator<MasteryList> getManyMasteryList(Map<String, Object> query, final PipelineContext context) {
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

        return get(MasteryList.class, UniqueKeys.forManyMasteryListDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Match.class)
    public CloseableIterator<Match> getManyMatch(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("matchIds");
        Utilities.checkNotNull(platform, "platform", iter, "matchIds");

        return get(Match.class, UniqueKeys.forManyMatchDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(MatchTimeline.class)
    public CloseableIterator<MatchTimeline> getManyMatchTimeline(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("matchIds");
        Utilities.checkNotNull(platform, "platform", iter, "matchIds");

        return get(MatchTimeline.class, UniqueKeys.forManyMatchTimelineDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ProfileIconData.class)
    public CloseableIterator<ProfileIconData> getManyProfileIconData(Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.containsKey("locale")) {
            query = new HashMap<>(query);
            query.put("locale", platform.getDefaultLocale());
        }

        return get(ProfileIconData.class, UniqueKeys.forManyProfileIconDataDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ProfileIconDetails.class)
    public CloseableIterator<ProfileIconDetails> getManyProfileIconDetails(Map<String, Object> query, final PipelineContext context) {
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

        return get(ProfileIconDetails.class, UniqueKeys.forManyProfileIconDetailsDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Realm.class)
    public CloseableIterator<Realm> getManyRealm(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(Realm.class, UniqueKeys.forManyRealmDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ReforgedRune.class)
    public CloseableIterator<ReforgedRune> getManyReforgedRune(Map<String, Object> query, final PipelineContext context) {
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

        return get(ReforgedRune.class, UniqueKeys.forManyReforgedRuneDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ReforgedRuneTree.class)
    public CloseableIterator<ReforgedRuneTree> getManyReforgedRuneTree(Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("versions");
        Utilities.checkNotNull(platform, "platform", iter, "versions");

        if(!query.containsKey("locale")) {
            query = new HashMap<>(query);
            query.put("locale", platform.getDefaultLocale());
        }

        return get(ReforgedRuneTree.class, UniqueKeys.forManyReforgedRuneTreeDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Rune.class)
    public CloseableIterator<Rune> getManyRune(Map<String, Object> query, final PipelineContext context) {
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

        return get(Rune.class, UniqueKeys.forManyRuneDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(RuneList.class)
    public CloseableIterator<RuneList> getManyRuneList(Map<String, Object> query, final PipelineContext context) {
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

        return get(RuneList.class, UniqueKeys.forManyRuneListDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(ShardStatus.class)
    public CloseableIterator<ShardStatus> getManyShardStatus(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(ShardStatus.class, UniqueKeys.forManyShardStatusDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Summoner.class)
    public CloseableIterator<Summoner> getManySummoner(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Iterable<Number> summonerIds = (Iterable<Number>)query.get("ids");
        final Iterable<Number> accountIds = (Iterable<Number>)query.get("accountIds");
        final Iterable<String> summonerNames = (Iterable<String>)query.get("names");
        Utilities.checkAtLeastOneNotNull(summonerIds, "ids", accountIds, "accountIds", summonerNames, "names");

        return get(Summoner.class, UniqueKeys.forManySummonerDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerPositions.class)
    public CloseableIterator<SummonerPositions> getManySummonerPositions(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(SummonerPositions.class, UniqueKeys.forManySummonerPositionsDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerSpell.class)
    public CloseableIterator<SummonerSpell> getManySummonerSpell(Map<String, Object> query, final PipelineContext context) {
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

        return get(SummonerSpell.class, UniqueKeys.forManySummonerSpellDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(SummonerSpellList.class)
    public CloseableIterator<SummonerSpellList> getManySummonerSpellList(Map<String, Object> query, final PipelineContext context) {
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

        return get(SummonerSpellList.class, UniqueKeys.forManySummonerSpellListDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(TournamentMatches.class)
    public CloseableIterator<TournamentMatches> getManyTournamentMatches(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<String> iter = (Iterable<String>)query.get("tournamentCodes");
        Utilities.checkNotNull(platform, "platform", iter, "tournamentCodes");

        return get(TournamentMatches.class, UniqueKeys.forManyTournamentMatchesDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(VerificationString.class)
    public CloseableIterator<VerificationString> getManyVerificationString(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(VerificationString.class, UniqueKeys.forManyVerificationStringDtoQuery(query));
    }

    @SuppressWarnings("unchecked")
    @GetMany(Versions.class)
    public CloseableIterator<Versions> getManyVersions(final Map<String, Object> query, final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(Versions.class, UniqueKeys.forManyVersionsDtoQuery(query));
    }

    @Get(MapData.class)
    public MapData getMapData(Map<String, Object> query, final PipelineContext context) {
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

        return get(MapData.class, UniqueKeys.forMapDataDtoQuery(query));
    }

    @Get(MapDetails.class)
    public MapDetails getMapDetails(Map<String, Object> query, final PipelineContext context) {
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

        return get(MapDetails.class, UniqueKeys.forMapDetailsDtoQuery(query));
    }

    @Get(Mastery.class)
    public Mastery getMastery(Map<String, Object> query, final PipelineContext context) {
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

        return get(Mastery.class, UniqueKeys.forMasteryDtoQuery(query));
    }

    @Get(MasteryList.class)
    public MasteryList getMasteryList(Map<String, Object> query, final PipelineContext context) {
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

        return get(MasteryList.class, UniqueKeys.forMasteryListDtoQuery(query));
    }

    @Get(Match.class)
    public Match getMatch(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number matchId = (Number)query.get("matchId");
        Utilities.checkNotNull(platform, "platform", matchId, "matchId");

        return get(Match.class, UniqueKeys.forMatchDtoQuery(query));
    }

    @Get(MatchTimeline.class)
    public MatchTimeline getMatchTimeline(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number matchId = (Number)query.get("matchId");
        Utilities.checkNotNull(platform, "platform", matchId, "matchId");

        return get(MatchTimeline.class, UniqueKeys.forMatchTimelineDtoQuery(query));
    }

    @Get(ProfileIconData.class)
    public ProfileIconData getProfileIconData(Map<String, Object> query, final PipelineContext context) {
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

        return get(ProfileIconData.class, UniqueKeys.forProfileIconDataDtoQuery(query));
    }

    @Get(ProfileIconDetails.class)
    public ProfileIconDetails getProfileIconDetails(Map<String, Object> query, final PipelineContext context) {
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

        return get(ProfileIconDetails.class, UniqueKeys.forProfileIconDetailsDtoQuery(query));
    }

    @Get(Realm.class)
    public Realm getRealm(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(Realm.class, UniqueKeys.forRealmDtoQuery(query));
    }

    @Get(ReforgedRune.class)
    public ReforgedRune getReforgedRune(Map<String, Object> query, final PipelineContext context) {
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

        return get(ReforgedRune.class, UniqueKeys.forReforgedRuneDtoQuery(query));
    }

    @Get(ReforgedRuneTree.class)
    public ReforgedRuneTree getReforgedRuneTree(Map<String, Object> query, final PipelineContext context) {
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

        return get(ReforgedRuneTree.class, UniqueKeys.forReforgedRuneTreeDtoQuery(query));
    }

    @Get(Rune.class)
    public Rune getRune(Map<String, Object> query, final PipelineContext context) {
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

        return get(Rune.class, UniqueKeys.forRuneDtoQuery(query));
    }

    @Get(RuneList.class)
    public RuneList getRuneList(Map<String, Object> query, final PipelineContext context) {
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

        return get(RuneList.class, UniqueKeys.forRuneListDtoQuery(query));
    }

    @Get(ShardStatus.class)
    public ShardStatus getShardStatus(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(ShardStatus.class, UniqueKeys.forShardStatusDtoQuery(query));
    }

    @Get(Summoner.class)
    public Summoner getSummoner(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");
        final Number summonerId = (Number)query.get("id");
        final Number accountId = (Number)query.get("accountId");
        final String summonerName = (String)query.get("name");
        Utilities.checkAtLeastOneNotNull(summonerId, "id", accountId, "accountId", summonerName, "name");

        return get(Summoner.class, UniqueKeys.forSummonerDtoQuery(query));
    }

    @Get(SummonerPositions.class)
    public SummonerPositions getSummonerPositions(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(SummonerPositions.class, UniqueKeys.forSummonerPositionsDtoQuery(query));
    }

    @Get(SummonerSpell.class)
    public SummonerSpell getSummonerSpell(Map<String, Object> query, final PipelineContext context) {
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

        return get(SummonerSpell.class, UniqueKeys.forSummonerSpellDtoQuery(query));
    }

    @Get(SummonerSpellList.class)
    public SummonerSpellList getSummonerSpellList(Map<String, Object> query, final PipelineContext context) {
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

        return get(SummonerSpellList.class, UniqueKeys.forSummonerSpellListDtoQuery(query));
    }

    @Get(TournamentMatches.class)
    public TournamentMatches getTournamentMatches(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final String tournamentCode = (String)query.get("tournamentCode");
        Utilities.checkNotNull(platform, "platform", tournamentCode, "tournamentCode");

        return get(TournamentMatches.class, UniqueKeys.forTournamentMatchesDtoQuery(query));
    }

    @Get(VerificationString.class)
    public VerificationString getVerificationString(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(VerificationString.class, UniqueKeys.forVerificationStringDtoQuery(query));
    }

    @Get(Versions.class)
    public Versions getVersions(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(Versions.class, UniqueKeys.forVersionsDtoQuery(query));
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

    private <T extends DataObject> void put(final Class<T> clazz, final int key, final T value) {
        final Transaction transaction = xodus.beginTransaction();
        try {
            final Store store = xodus.openStore(clazz.getCanonicalName(), STORE_CONFIG, transaction);
            store.put(transaction, IntegerBinding.intToEntry(key), toByteIterable(value));
        } finally {
            transaction.commit();
        }
    }

    private <T extends DataObject> void put(final Class<T> clazz, final Iterable<Integer> keys, final Iterable<T> values) {
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
        for(final int key : UniqueKeys.forChampionDto(champion)) {
            keys.add(key);
            values.add(champion);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Champion.class, keys, values);
    }

    @Put(ChampionList.class)
    public void putChampionList(final ChampionList list, final PipelineContext context) {
        put(ChampionList.class, UniqueKeys.forChampionListDto(list), list);
        putManyChampion(list.getData().values(), context);
    }

    @Put(ChampionMasteries.class)
    public void putChampionMasteries(final ChampionMasteries masteries, final PipelineContext context) {
        put(ChampionMasteries.class, UniqueKeys.forChampionMasteriesDto(masteries), masteries);
        putManyChampionMastery(masteries, context);
    }

    @Put(ChampionMastery.class)
    public void putChampionMastery(final ChampionMastery mastery, final PipelineContext context) {
        put(ChampionMastery.class, UniqueKeys.forChampionMasteryDto(mastery), mastery);
    }

    @Put(ChampionMasteryScore.class)
    public void putChampionMasteryScore(final ChampionMasteryScore score, final PipelineContext context) {
        put(ChampionMasteryScore.class, UniqueKeys.forChampionMasteryScoreDto(score), score);
    }

    @Put(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public void putChampionStatus(final com.merakianalytics.orianna.types.dto.champion.Champion champion, final PipelineContext context) {
        put(com.merakianalytics.orianna.types.dto.champion.Champion.class, UniqueKeys.forChampionStatusDto(champion), champion);
    }

    @Put(com.merakianalytics.orianna.types.dto.champion.ChampionList.class)
    public void putChampionStatusList(final com.merakianalytics.orianna.types.dto.champion.ChampionList champions, final PipelineContext context) {
        put(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, UniqueKeys.forChampionStatusListDto(champions), champions);
        putManyChampionStatus(champions.getChampions(), context);
    }

    @Put(CurrentGameInfo.class)
    public void putCurrentGameInfo(final CurrentGameInfo game, final PipelineContext context) {
        put(CurrentGameInfo.class, UniqueKeys.forCurrentGameInfoDto(game), game);
    }

    @Put(FeaturedGames.class)
    public void putFeaturedGames(final FeaturedGames games, final PipelineContext context) {
        put(FeaturedGames.class, UniqueKeys.forFeaturedGamesDto(games), games);
    }

    @Put(Item.class)
    public void putItem(final Item item, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Item> values = new ArrayList<>();
        for(final int key : UniqueKeys.forItemDto(item)) {
            keys.add(key);
            values.add(item);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Item.class, keys, values);
    }

    @Put(ItemList.class)
    public void putItemList(final ItemList list, final PipelineContext context) {
        put(ItemList.class, UniqueKeys.forItemListDto(list), list);
        putManyItem(list.getData().values(), context);
    }

    @Put(Languages.class)
    public void putLanguages(final Languages languages, final PipelineContext context) {
        put(Languages.class, UniqueKeys.forLanguagesDto(languages), languages);
    }

    @Put(LanguageStrings.class)
    public void putLanguageStrings(final LanguageStrings strings, final PipelineContext context) {
        put(LanguageStrings.class, UniqueKeys.forLanguageStringsDto(strings), strings);
    }

    @Put(LeagueList.class)
    public void putLeagueList(final LeagueList list, final PipelineContext context) {
        final int[] keys = UniqueKeys.forLeagueListDto(list);
        final List<Integer> keyList = new ArrayList<>(keys.length);
        final List<LeagueList> valueList = new ArrayList<>(keys.length);
        for(final int key : keys) {
            keyList.add(key);
            valueList.add(list);
        }

        put(LeagueList.class, keyList, valueList);
    }

    @PutMany(Summoner.class)
    public void putManSummoner(final Iterable<Summoner> summoners, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Summoner> values = new ArrayList<>();
        for(final Summoner summoner : summoners) {
            for(final int key : UniqueKeys.forSummonerDto(summoner)) {
                keys.add(key);
                values.add(summoner);
            }
        }
        keys.trimToSize();

        put(Summoner.class, keys, values);
    }

    @PutMany(Champion.class)
    public void putManyChampion(final Iterable<Champion> champions, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Champion> values = new ArrayList<>();
        for(final Champion champion : champions) {
            for(final int key : UniqueKeys.forChampionDto(champion)) {
                keys.add(key);
                values.add(champion);
            }
        }
        keys.trimToSize();

        put(Champion.class, keys, values);
    }

    @PutMany(ChampionList.class)
    public void putManyChampionList(final Iterable<ChampionList> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ChampionList list : lists) {
            keys.add(UniqueKeys.forChampionListDto(list));
        }
        keys.trimToSize();

        put(ChampionList.class, keys, lists);
        final List<List<Champion>> toStore = new ArrayList<>(keys.size());
        for(final ChampionList list : lists) {
            toStore.add(new ArrayList<>(list.getData().values()));
        }
        putManyChampion(Iterables.concat(toStore), context);
    }

    @PutMany(ChampionMasteries.class)
    public void putManyChampionMasteries(final Iterable<ChampionMasteries> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ChampionMasteries list : lists) {
            keys.add(UniqueKeys.forChampionMasteriesDto(list));
        }
        keys.trimToSize();

        put(ChampionMasteries.class, keys, lists);
        putManyChampionMastery(Iterables.concat(lists), context);
    }

    @PutMany(ChampionMastery.class)
    public void putManyChampionMastery(final Iterable<ChampionMastery> masteries, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ChampionMastery mastery : masteries) {
            keys.add(UniqueKeys.forChampionMasteryDto(mastery));
        }
        keys.trimToSize();

        put(ChampionMastery.class, keys, masteries);
    }

    @PutMany(ChampionMasteryScore.class)
    public void putManyChampionMasteryScore(final Iterable<ChampionMasteryScore> scores, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ChampionMasteryScore score : scores) {
            keys.add(UniqueKeys.forChampionMasteryScoreDto(score));
        }
        keys.trimToSize();

        put(ChampionMasteryScore.class, keys, scores);
    }

    @PutMany(com.merakianalytics.orianna.types.dto.champion.Champion.class)
    public void putManyChampionStatus(final Iterable<com.merakianalytics.orianna.types.dto.champion.Champion> champions, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final com.merakianalytics.orianna.types.dto.champion.Champion champion : champions) {
            keys.add(UniqueKeys.forChampionStatusDto(champion));
        }
        keys.trimToSize();

        put(com.merakianalytics.orianna.types.dto.champion.Champion.class, keys, champions);
    }

    @PutMany(com.merakianalytics.orianna.types.dto.champion.ChampionList.class)
    public void putManyChampionStatusList(final Iterable<com.merakianalytics.orianna.types.dto.champion.ChampionList> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final com.merakianalytics.orianna.types.dto.champion.ChampionList list : lists) {
            keys.add(UniqueKeys.forChampionStatusListDto(list));
        }
        keys.trimToSize();

        put(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, keys, lists);
        final List<List<com.merakianalytics.orianna.types.dto.champion.Champion>> toStore = new ArrayList<>(keys.size());
        for(final com.merakianalytics.orianna.types.dto.champion.ChampionList list : lists) {
            toStore.add(list.getChampions());
        }
        putManyChampionStatus(Iterables.concat(toStore), context);
    }

    @PutMany(CurrentGameInfo.class)
    public void putManyCurrentGameInfo(final Iterable<CurrentGameInfo> games, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final CurrentGameInfo game : games) {
            keys.add(UniqueKeys.forCurrentGameInfoDto(game));
        }
        keys.trimToSize();

        put(CurrentGameInfo.class, keys, games);
    }

    @PutMany(FeaturedGames.class)
    public void putManyFeaturedGames(final Iterable<FeaturedGames> games, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final FeaturedGames game : games) {
            keys.add(UniqueKeys.forFeaturedGamesDto(game));
        }
        keys.trimToSize();

        put(FeaturedGames.class, keys, games);
    }

    @PutMany(Item.class)
    public void putManyItem(final Iterable<Item> items, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Item> values = new ArrayList<>();
        for(final Item item : items) {
            for(final int key : UniqueKeys.forItemDto(item)) {
                keys.add(key);
                values.add(item);
            }
        }
        keys.trimToSize();

        put(Item.class, keys, values);
    }

    @PutMany(ItemList.class)
    public void putManyItemList(final Iterable<ItemList> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ItemList list : lists) {
            keys.add(UniqueKeys.forItemListDto(list));
        }
        keys.trimToSize();

        put(ItemList.class, keys, lists);
        final List<List<Item>> toStore = new ArrayList<>(keys.size());
        for(final ItemList list : lists) {
            toStore.add(new ArrayList<>(list.getData().values()));
        }
        putManyItem(Iterables.concat(toStore), context);
    }

    @PutMany(Languages.class)
    public void putManyLanguages(final Iterable<Languages> languages, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Languages language : languages) {
            keys.add(UniqueKeys.forLanguagesDto(language));
        }
        keys.trimToSize();

        put(Languages.class, keys, languages);
    }

    @PutMany(LanguageStrings.class)
    public void putManyLanguageStrings(final Iterable<LanguageStrings> strings, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final LanguageStrings string : strings) {
            keys.add(UniqueKeys.forLanguageStringsDto(string));
        }
        keys.trimToSize();

        put(LanguageStrings.class, keys, strings);
    }

    @PutMany(LeagueList.class)
    public void putManyLeagueList(final Iterable<LeagueList> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<LeagueList> values = new ArrayList<>();
        for(final LeagueList list : lists) {
            for(final int key : UniqueKeys.forLeagueListDto(list)) {
                keys.add(key);
                values.add(list);
            }
        }
        keys.trimToSize();
        values.trimToSize();

        put(LeagueList.class, keys, values);
    }

    @PutMany(MapData.class)
    public void putManyMapData(final Iterable<MapData> maps, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final MapData map : maps) {
            keys.add(UniqueKeys.forMapDataDto(map));
        }
        keys.trimToSize();

        put(MapData.class, keys, maps);
        final List<List<MapDetails>> toStore = new ArrayList<>(keys.size());
        for(final MapData map : maps) {
            toStore.add(new ArrayList<>(map.getData().values()));
        }
        putManyMapDetails(Iterables.concat(toStore), context);
    }

    @PutMany(MapDetails.class)
    public void putManyMapDetails(final Iterable<MapDetails> maps, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<MapDetails> values = new ArrayList<>();
        for(final MapDetails map : maps) {
            for(final int key : UniqueKeys.forMapDetailsDto(map)) {
                keys.add(key);
                values.add(map);
            }
        }
        keys.trimToSize();

        put(MapDetails.class, keys, values);
    }

    @PutMany(Mastery.class)
    public void putManyMastery(final Iterable<Mastery> items, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Mastery> values = new ArrayList<>();
        for(final Mastery item : items) {
            for(final int key : UniqueKeys.forMasteryDto(item)) {
                keys.add(key);
                values.add(item);
            }
        }
        keys.trimToSize();

        put(Mastery.class, keys, values);
    }

    @PutMany(MasteryList.class)
    public void putManyMasteryList(final Iterable<MasteryList> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final MasteryList list : lists) {
            keys.add(UniqueKeys.forMasteryListDto(list));
        }
        keys.trimToSize();

        put(MasteryList.class, keys, lists);
        final List<List<Mastery>> toStore = new ArrayList<>();
        for(final MasteryList list : lists) {
            toStore.add(new ArrayList<>(list.getData().values()));
        }
        putManyMastery(Iterables.concat(toStore), context);
    }

    @PutMany(Match.class)
    public void putManyMatch(final Iterable<Match> matches, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Match match : matches) {
            keys.add(UniqueKeys.forMatchDto(match));
        }
        keys.trimToSize();

        put(Match.class, keys, matches);
    }

    @PutMany(MatchTimeline.class)
    public void putManyMatchTimeline(final Iterable<MatchTimeline> timelines, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final MatchTimeline timeline : timelines) {
            keys.add(UniqueKeys.forMatchTimelineDto(timeline));
        }
        keys.trimToSize();

        put(MatchTimeline.class, keys, timelines);
    }

    @PutMany(ProfileIconData.class)
    public void putManyProfileIconData(final Iterable<ProfileIconData> icons, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ProfileIconData icon : icons) {
            keys.add(UniqueKeys.forProfileIconDataDto(icon));
        }
        keys.trimToSize();

        put(ProfileIconData.class, keys, icons);
        final List<List<ProfileIconDetails>> toStore = new ArrayList<>(keys.size());
        for(final ProfileIconData map : icons) {
            toStore.add(new ArrayList<>(map.getData().values()));
        }
        putManyProfileIconDetails(Iterables.concat(toStore), context);
    }

    @PutMany(ProfileIconDetails.class)
    public void putManyProfileIconDetails(final Iterable<ProfileIconDetails> icons, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ProfileIconDetails icon : icons) {
            keys.add(UniqueKeys.forProfileIconDetailsDto(icon));
        }
        keys.trimToSize();

        put(ProfileIconDetails.class, keys, icons);
    }

    @PutMany(Realm.class)
    public void putManyRealm(final Iterable<Realm> realms, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Realm realm : realms) {
            keys.add(UniqueKeys.forRealmDto(realm));
        }
        keys.trimToSize();

        put(Realm.class, keys, realms);
    }

    @PutMany(ReforgedRune.class)
    public void putManyReforgedRune(final Iterable<ReforgedRune> runes, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<ReforgedRune> values = new ArrayList<>();
        for(final ReforgedRune rune : runes) {
            for(final int key : UniqueKeys.forReforgedRuneDto(rune)) {
                keys.add(key);
                values.add(rune);
            }
        }
        keys.trimToSize();

        put(ReforgedRune.class, keys, values);
    }

    @PutMany(ReforgedRuneTree.class)
    public void putManyReforgedRuneTree(final Iterable<ReforgedRuneTree> trees, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ReforgedRuneTree tree : trees) {
            keys.add(UniqueKeys.forReforgedRuneTreeDto(tree));
        }
        keys.trimToSize();

        put(ReforgedRuneTree.class, keys, trees);
        final List<ReforgedRune> toStore = new ArrayList<>(keys.size());
        for(final ReforgedRuneTree tree : trees) {
            for(final ReforgedRunePath path : tree) {
                for(final ReforgedRuneSlot slot : path.getSlots()) {
                    for(final ReforgedRune rune : slot.getRunes()) {
                        toStore.add(rune);
                    }
                }
            }
        }
        putManyReforgedRune(toStore, context);
    }

    @PutMany(Rune.class)
    public void putManyRune(final Iterable<Rune> runes, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Rune> values = new ArrayList<>();
        for(final Rune rune : runes) {
            for(final int key : UniqueKeys.forRuneDto(rune)) {
                keys.add(key);
                values.add(rune);
            }
        }
        keys.trimToSize();

        put(Rune.class, keys, values);
    }

    @PutMany(RuneList.class)
    public void putManyRuneList(final Iterable<RuneList> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final RuneList list : lists) {
            keys.add(UniqueKeys.forRuneListDto(list));
        }
        keys.trimToSize();

        put(RuneList.class, keys, lists);
        final List<List<Rune>> toStore = new ArrayList<>(keys.size());
        for(final RuneList list : lists) {
            toStore.add(new ArrayList<>(list.getData().values()));
        }
        putManyRune(Iterables.concat(toStore), context);
    }

    @PutMany(ShardStatus.class)
    public void putManyShardStatus(final Iterable<ShardStatus> statuses, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final ShardStatus status : statuses) {
            keys.add(UniqueKeys.forShardStatusDto(status));
        }
        keys.trimToSize();

        put(ShardStatus.class, keys, statuses);
    }

    @PutMany(SummonerPositions.class)
    public void putManySummonerPositions(final Iterable<SummonerPositions> positions, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final SummonerPositions position : positions) {
            keys.add(UniqueKeys.forSummonerPositionsDto(position));
        }
        keys.trimToSize();

        put(SummonerPositions.class, keys, positions);
    }

    @PutMany(SummonerSpell.class)
    public void putManySummonerSpell(final Iterable<SummonerSpell> summonerSpells, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<SummonerSpell> values = new ArrayList<>();
        for(final SummonerSpell summonerSpell : summonerSpells) {
            for(final int key : UniqueKeys.forSummonerSpellDto(summonerSpell)) {
                keys.add(key);
                values.add(summonerSpell);
            }
        }
        keys.trimToSize();

        put(SummonerSpell.class, keys, values);
    }

    @PutMany(SummonerSpellList.class)
    public void putManySummonerSpellList(final Iterable<SummonerSpellList> lists, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final SummonerSpellList list : lists) {
            keys.add(UniqueKeys.forSummonerSpellListDto(list));
        }
        keys.trimToSize();

        put(SummonerSpellList.class, keys, lists);
        final List<List<SummonerSpell>> toStore = new ArrayList<>(keys.size());
        for(final SummonerSpellList list : lists) {
            toStore.add(new ArrayList<>(list.getData().values()));
        }
        putManySummonerSpell(Iterables.concat(toStore), context);
    }

    @PutMany(TournamentMatches.class)
    public void putManyTournamentMatches(final Iterable<TournamentMatches> matches, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final TournamentMatches match : matches) {
            keys.add(UniqueKeys.forTournamentMatchesDto(match));
        }
        keys.trimToSize();

        put(TournamentMatches.class, keys, matches);
    }

    @PutMany(VerificationString.class)
    public void putManyVerificationString(final Iterable<VerificationString> strings, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final VerificationString string : strings) {
            keys.add(UniqueKeys.forVerificationStringDto(string));
        }
        keys.trimToSize();

        put(VerificationString.class, keys, strings);
    }

    @PutMany(Versions.class)
    public void putManyVersions(final Iterable<Versions> versions, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final Versions version : versions) {
            keys.add(UniqueKeys.forVersionsDto(version));
        }
        keys.trimToSize();

        put(Versions.class, keys, versions);
    }

    @Put(MapData.class)
    public void putMapData(final MapData data, final PipelineContext context) {
        put(MapData.class, UniqueKeys.forMapDataDto(data), data);
        putManyMapDetails(data.getData().values(), context);
    }

    @Put(MapDetails.class)
    public void putMapDetails(final MapDetails map, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<MapDetails> values = new ArrayList<>();
        for(final int key : UniqueKeys.forMapDetailsDto(map)) {
            keys.add(key);
            values.add(map);
        }
        keys.trimToSize();
        values.trimToSize();

        put(MapDetails.class, keys, values);
    }

    @Put(Mastery.class)
    public void putMastery(final Mastery mastery, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Mastery> values = new ArrayList<>();
        for(final int key : UniqueKeys.forMasteryDto(mastery)) {
            keys.add(key);
            values.add(mastery);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Mastery.class, keys, values);
    }

    @Put(MasteryList.class)
    public void putMasteryList(final MasteryList list, final PipelineContext context) {
        put(MasteryList.class, UniqueKeys.forMasteryListDto(list), list);
        putManyMastery(list.getData().values(), context);
    }

    @Put(Match.class)
    public void putMatch(final Match match, final PipelineContext context) {
        put(Match.class, UniqueKeys.forMatchDto(match), match);
    }

    @Put(MatchTimeline.class)
    public void putMatchTimeline(final MatchTimeline timeline, final PipelineContext context) {
        put(MatchTimeline.class, UniqueKeys.forMatchTimelineDto(timeline), timeline);
    }

    @Put(ProfileIconData.class)
    public void putProfileIconData(final ProfileIconData data, final PipelineContext context) {
        put(ProfileIconData.class, UniqueKeys.forProfileIconDataDto(data), data);
        putManyProfileIconDetails(data.getData().values(), context);
    }

    @Put(ProfileIconDetails.class)
    public void putProfileIconDetails(final ProfileIconDetails icon, final PipelineContext context) {
        put(ProfileIconDetails.class, UniqueKeys.forProfileIconDetailsDto(icon), icon);
    }

    @Put(Realm.class)
    public void putRealm(final Realm realm, final PipelineContext context) {
        put(Realm.class, UniqueKeys.forRealmDto(realm), realm);
    }

    @Put(ReforgedRune.class)
    public void putReforgedRune(final ReforgedRune rune, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<ReforgedRune> values = new ArrayList<>();
        for(final int key : UniqueKeys.forReforgedRuneDto(rune)) {
            keys.add(key);
            values.add(rune);
        }
        keys.trimToSize();
        values.trimToSize();

        put(ReforgedRune.class, keys, values);
    }

    @Put(ReforgedRuneTree.class)
    public void putReforgedRuneTree(final ReforgedRuneTree tree, final PipelineContext context) {
        put(ReforgedRuneTree.class, UniqueKeys.forReforgedRuneTreeDto(tree), tree);
        final List<ReforgedRune> toStore = new ArrayList<>();
        for(final ReforgedRunePath path : tree) {
            for(final ReforgedRuneSlot slot : path.getSlots()) {
                for(final ReforgedRune rune : slot.getRunes()) {
                    toStore.add(rune);
                }
            }
        }
        putManyReforgedRune(toStore, context);
    }

    @Put(Rune.class)
    public void putRune(final Rune rune, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Rune> values = new ArrayList<>();
        for(final int key : UniqueKeys.forRuneDto(rune)) {
            keys.add(key);
            values.add(rune);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Rune.class, keys, values);
    }

    @Put(RuneList.class)
    public void putRuneList(final RuneList list, final PipelineContext context) {
        put(RuneList.class, UniqueKeys.forRuneListDto(list), list);
        putManyRune(list.getData().values(), context);
    }

    @Put(ShardStatus.class)
    public void putShardStatus(final ShardStatus status, final PipelineContext context) {
        put(ShardStatus.class, UniqueKeys.forShardStatusDto(status), status);
    }

    @Put(Summoner.class)
    public void putSummoner(final Summoner summoner, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<Summoner> values = new ArrayList<>();
        for(final int key : UniqueKeys.forSummonerDto(summoner)) {
            keys.add(key);
            values.add(summoner);
        }
        keys.trimToSize();
        values.trimToSize();

        put(Summoner.class, keys, values);
    }

    @Put(SummonerPositions.class)
    public void putSummonerPositions(final SummonerPositions positions, final PipelineContext context) {
        put(SummonerPositions.class, UniqueKeys.forSummonerPositionsDto(positions), positions);
    }

    @Put(SummonerSpell.class)
    public void putSummonerSpell(final SummonerSpell summonerSpell, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        final ArrayList<SummonerSpell> values = new ArrayList<>();
        for(final int key : UniqueKeys.forSummonerSpellDto(summonerSpell)) {
            keys.add(key);
            values.add(summonerSpell);
        }
        keys.trimToSize();
        values.trimToSize();

        put(SummonerSpell.class, keys, values);
    }

    @Put(SummonerSpellList.class)
    public void putSummonerSpellList(final SummonerSpellList list, final PipelineContext context) {
        put(SummonerSpellList.class, UniqueKeys.forSummonerSpellListDto(list), list);
        putManySummonerSpell(list.getData().values(), context);
    }

    @Put(TournamentMatches.class)
    public void putTournamentMatches(final TournamentMatches matches, final PipelineContext context) {
        put(TournamentMatches.class, UniqueKeys.forTournamentMatchesDto(matches), matches);
    }

    @Put(VerificationString.class)
    public void putVerificationString(final VerificationString string, final PipelineContext context) {
        put(VerificationString.class, UniqueKeys.forVerificationStringDto(string), string);
    }

    @Put(Versions.class)
    public void putVersions(final Versions versions, final PipelineContext context) {
        put(Versions.class, UniqueKeys.forVersionsDto(versions), versions);
    }
}

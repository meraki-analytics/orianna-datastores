package com.merakianalytics.orianna.datastores.xodus.dto;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import com.merakianalytics.orianna.datapipeline.common.QueryValidationException;
import com.merakianalytics.orianna.datapipeline.common.Utilities;
import com.merakianalytics.orianna.datapipeline.common.expiration.ExpirationPeriod;
import com.merakianalytics.orianna.types.UniqueKeys;
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

    private static final StoreConfig STORE_CONFIG = StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;

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
    public com.merakianalytics.orianna.types.dto.champion.ChampionList getChampionStatusList(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        Utilities.checkNotNull(platform, "platform");

        return get(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, UniqueKeys.forChampionStatusListDtoQuery(query));
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
    public CloseableIterator<com.merakianalytics.orianna.types.dto.champion.ChampionList> getManyChampionStatusList(final Map<String, Object> query,
        final PipelineContext context) {
        final Iterable<Platform> iter = (Iterable<Platform>)query.get("platforms");
        Utilities.checkNotNull(iter, "platforms");

        return get(com.merakianalytics.orianna.types.dto.champion.ChampionList.class, UniqueKeys.forManyChampionStatusListDtoQuery(query));
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
    @GetMany(SummonerPositions.class)
    public CloseableIterator<SummonerPositions> getManySummonerPositions(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Iterable<Number> iter = (Iterable<Number>)query.get("summonerIds");
        Utilities.checkNotNull(platform, "platform", iter, "summonerIds");

        return get(SummonerPositions.class, UniqueKeys.forManySummonerPositionsDtoQuery(query));
    }

    @Get(SummonerPositions.class)
    public SummonerPositions getSummonerPositions(final Map<String, Object> query, final PipelineContext context) {
        final Platform platform = (Platform)query.get("platform");
        final Number summonerId = (Number)query.get("summonerId");
        Utilities.checkNotNull(platform, "platform", summonerId, "summonerId");

        return get(SummonerPositions.class, UniqueKeys.forSummonerPositionsDtoQuery(query));
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
                store.put(transaction, IntegerBinding.intToEntry(keyIterator.next()), new ArrayByteIterable(toByteIterable(valueIterator.next())));
            }
        } finally {
            transaction.commit();
        }
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

    @PutMany(SummonerPositions.class)
    public void putManySummonerPositions(final Iterable<SummonerPositions> positions, final PipelineContext context) {
        final ArrayList<Integer> keys = new ArrayList<>();
        for(final SummonerPositions position : positions) {
            keys.add(UniqueKeys.forSummonerPositionsDto(position));
        }
        keys.trimToSize();

        put(SummonerPositions.class, keys, positions);
    }

    @Put(SummonerPositions.class)
    public void putSummonerPositions(final SummonerPositions positions, final PipelineContext context) {
        put(SummonerPositions.class, UniqueKeys.forSummonerPositionsDto(positions), positions);
    }
}

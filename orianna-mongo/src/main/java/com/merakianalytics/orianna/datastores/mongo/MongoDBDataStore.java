package com.merakianalytics.orianna.datastores.mongo;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.merakianalytics.datapipelines.AbstractDataStore;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public abstract class MongoDBDataStore extends AbstractDataStore implements AutoCloseable {
    public static class Configuration {
        private int connectionsPerHost = 100;
        private long connectTimeout = 10000L;
        private TimeUnit connectTimeoutUnit = TimeUnit.MILLISECONDS;
        private String database = "orianna";
        private long heartbeatConnectTimeout = 20000L;
        private TimeUnit heartbeatConnectTimeoutUnit = TimeUnit.MILLISECONDS;
        private long heartbeatFrequency = 500L;
        private TimeUnit heartbeatFrequencyUnit = TimeUnit.MILLISECONDS;
        private long heartbeatSocketTimeout = 20000L;
        private TimeUnit heartbeatSocketTimeoutUnit = TimeUnit.MILLISECONDS;
        private String host = "localhost";
        private long localThreshold = 15L;
        private TimeUnit localThresholdUnit = TimeUnit.MILLISECONDS;
        private long maxConnectionIdleTime = 0L;
        private TimeUnit maxConnectionIdleTimeUnit = TimeUnit.MILLISECONDS;
        private long maxConnectionLifeTime = 0L;
        private TimeUnit maxConnectionLifeTimeUnit = TimeUnit.MILLISECONDS;
        private long maxWaitTime = 120000L;
        private TimeUnit maxWaitTimeUnit = TimeUnit.MILLISECONDS;
        private int minConnectionsPerHost = 0;
        private long minHeartbeatFrequency = 500L;
        private TimeUnit minHeartbeatFrequencyUnit = TimeUnit.MILLISECONDS;
        private String password = null;
        private int port = 27017;
        private ReadConcern readConcern = ReadConcern.LOCAL;
        private long serverSelectionTimeout = 30000L;
        private TimeUnit serverSelectionTimeoutUnit = TimeUnit.MILLISECONDS;
        private long socketTimeout = 0L;
        private TimeUnit socketTimeoutUnit = TimeUnit.MILLISECONDS;
        private boolean sslEnabled = false;
        private int threadsAllowedToBlockForConnectionMultiplier = 5;
        private String userName = null;
        private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

        /**
         * @return the connectionsPerHost
         */
        public int getConnectionsPerHost() {
            return connectionsPerHost;
        }

        /**
         * @return the connectTimeout
         */
        public long getConnectTimeout() {
            return connectTimeout;
        }

        /**
         * @return the connectTimeoutUnit
         */
        public TimeUnit getConnectTimeoutUnit() {
            return connectTimeoutUnit;
        }

        /**
         * @return the database
         */
        public String getDatabase() {
            return database;
        }

        /**
         * @return the heartbeatConnectTimeout
         */
        public long getHeartbeatConnectTimeout() {
            return heartbeatConnectTimeout;
        }

        /**
         * @return the heartbeatConnectTimeoutUnit
         */
        public TimeUnit getHeartbeatConnectTimeoutUnit() {
            return heartbeatConnectTimeoutUnit;
        }

        /**
         * @return the heartbeatFrequency
         */
        public long getHeartbeatFrequency() {
            return heartbeatFrequency;
        }

        /**
         * @return the heartbeatFrequencyUnit
         */
        public TimeUnit getHeartbeatFrequencyUnit() {
            return heartbeatFrequencyUnit;
        }

        /**
         * @return the heartbeatSocketTimeout
         */
        public long getHeartbeatSocketTimeout() {
            return heartbeatSocketTimeout;
        }

        /**
         * @return the heartbeatSocketTimeoutUnit
         */
        public TimeUnit getHeartbeatSocketTimeoutUnit() {
            return heartbeatSocketTimeoutUnit;
        }

        /**
         * @return the host
         */
        public String getHost() {
            return host;
        }

        /**
         * @return the localThreshold
         */
        public long getLocalThreshold() {
            return localThreshold;
        }

        /**
         * @return the localThresholdUnit
         */
        public TimeUnit getLocalThresholdUnit() {
            return localThresholdUnit;
        }

        /**
         * @return the maxConnectionIdleTime
         */
        public long getMaxConnectionIdleTime() {
            return maxConnectionIdleTime;
        }

        /**
         * @return the maxConnectionIdleTimeUnit
         */
        public TimeUnit getMaxConnectionIdleTimeUnit() {
            return maxConnectionIdleTimeUnit;
        }

        /**
         * @return the maxConnectionLifeTime
         */
        public long getMaxConnectionLifeTime() {
            return maxConnectionLifeTime;
        }

        /**
         * @return the maxConnectionLifeTimeUnit
         */
        public TimeUnit getMaxConnectionLifeTimeUnit() {
            return maxConnectionLifeTimeUnit;
        }

        /**
         * @return the maxWaitTime
         */
        public long getMaxWaitTime() {
            return maxWaitTime;
        }

        /**
         * @return the maxWaitTimeUnit
         */
        public TimeUnit getMaxWaitTimeUnit() {
            return maxWaitTimeUnit;
        }

        /**
         * @return the minConnectionsPerHost
         */
        public int getMinConnectionsPerHost() {
            return minConnectionsPerHost;
        }

        /**
         * @return the minHeartbeatFrequency
         */
        public long getMinHeartbeatFrequency() {
            return minHeartbeatFrequency;
        }

        /**
         * @return the minHeartbeatFrequencyUnit
         */
        public TimeUnit getMinHeartbeatFrequencyUnit() {
            return minHeartbeatFrequencyUnit;
        }

        /**
         * @return the password
         */
        public String getPassword() {
            return password;
        }

        /**
         * @return the port
         */
        public int getPort() {
            return port;
        }

        /**
         * @return the readConcern
         */
        public ReadConcern getReadConcern() {
            return readConcern;
        }

        /**
         * @return the serverSelectionTimeout
         */
        public long getServerSelectionTimeout() {
            return serverSelectionTimeout;
        }

        /**
         * @return the serverSelectionTimeoutUnit
         */
        public TimeUnit getServerSelectionTimeoutUnit() {
            return serverSelectionTimeoutUnit;
        }

        /**
         * @return the socketTimeout
         */
        public long getSocketTimeout() {
            return socketTimeout;
        }

        /**
         * @return the socketTimeoutUnit
         */
        public TimeUnit getSocketTimeoutUnit() {
            return socketTimeoutUnit;
        }

        /**
         * @return the threadsAllowedToBlockForConnectionMultiplier
         */
        public int getThreadsAllowedToBlockForConnectionMultiplier() {
            return threadsAllowedToBlockForConnectionMultiplier;
        }

        /**
         * @return the userName
         */
        public String getUserName() {
            return userName;
        }

        /**
         * @return the writeConcern
         */
        public WriteConcern getWriteConcern() {
            return writeConcern;
        }

        /**
         * @return the sslEnabled
         */
        public boolean isSslEnabled() {
            return sslEnabled;
        }

        /**
         * @param connectionsPerHost
         *        the connectionsPerHost to set
         */
        public void setConnectionsPerHost(final int connectionsPerHost) {
            this.connectionsPerHost = connectionsPerHost;
        }

        /**
         * @param connectTimeout
         *        the connectTimeout to set
         */
        public void setConnectTimeout(final long connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        /**
         * @param connectTimeoutUnit
         *        the connectTimeoutUnit to set
         */
        public void setConnectTimeoutUnit(final TimeUnit connectTimeoutUnit) {
            this.connectTimeoutUnit = connectTimeoutUnit;
        }

        /**
         * @param database
         *        the database to set
         */
        public void setDatabase(final String database) {
            this.database = database;
        }

        /**
         * @param heartbeatConnectTimeout
         *        the heartbeatConnectTimeout to set
         */
        public void setHeartbeatConnectTimeout(final long heartbeatConnectTimeout) {
            this.heartbeatConnectTimeout = heartbeatConnectTimeout;
        }

        /**
         * @param heartbeatConnectTimeoutUnit
         *        the heartbeatConnectTimeoutUnit to set
         */
        public void setHeartbeatConnectTimeoutUnit(final TimeUnit heartbeatConnectTimeoutUnit) {
            this.heartbeatConnectTimeoutUnit = heartbeatConnectTimeoutUnit;
        }

        /**
         * @param heartbeatFrequency
         *        the heartbeatFrequency to set
         */
        public void setHeartbeatFrequency(final long heartbeatFrequency) {
            this.heartbeatFrequency = heartbeatFrequency;
        }

        /**
         * @param heartbeatFrequencyUnit
         *        the heartbeatFrequencyUnit to set
         */
        public void setHeartbeatFrequencyUnit(final TimeUnit heartbeatFrequencyUnit) {
            this.heartbeatFrequencyUnit = heartbeatFrequencyUnit;
        }

        /**
         * @param heartbeatSocketTimeout
         *        the heartbeatSocketTimeout to set
         */
        public void setHeartbeatSocketTimeout(final long heartbeatSocketTimeout) {
            this.heartbeatSocketTimeout = heartbeatSocketTimeout;
        }

        /**
         * @param heartbeatSocketTimeoutUnit
         *        the heartbeatSocketTimeoutUnit to set
         */
        public void setHeartbeatSocketTimeoutUnit(final TimeUnit heartbeatSocketTimeoutUnit) {
            this.heartbeatSocketTimeoutUnit = heartbeatSocketTimeoutUnit;
        }

        /**
         * @param host
         *        the host to set
         */
        public void setHost(final String host) {
            this.host = host;
        }

        /**
         * @param localThreshold
         *        the localThreshold to set
         */
        public void setLocalThreshold(final long localThreshold) {
            this.localThreshold = localThreshold;
        }

        /**
         * @param localThresholdUnit
         *        the localThresholdUnit to set
         */
        public void setLocalThresholdUnit(final TimeUnit localThresholdUnit) {
            this.localThresholdUnit = localThresholdUnit;
        }

        /**
         * @param maxConnectionIdleTime
         *        the maxConnectionIdleTime to set
         */
        public void setMaxConnectionIdleTime(final long maxConnectionIdleTime) {
            this.maxConnectionIdleTime = maxConnectionIdleTime;
        }

        /**
         * @param maxConnectionIdleTimeUnit
         *        the maxConnectionIdleTimeUnit to set
         */
        public void setMaxConnectionIdleTimeUnit(final TimeUnit maxConnectionIdleTimeUnit) {
            this.maxConnectionIdleTimeUnit = maxConnectionIdleTimeUnit;
        }

        /**
         * @param maxConnectionLifeTime
         *        the maxConnectionLifeTime to set
         */
        public void setMaxConnectionLifeTime(final long maxConnectionLifeTime) {
            this.maxConnectionLifeTime = maxConnectionLifeTime;
        }

        /**
         * @param maxConnectionLifeTimeUnit
         *        the maxConnectionLifeTimeUnit to set
         */
        public void setMaxConnectionLifeTimeUnit(final TimeUnit maxConnectionLifeTimeUnit) {
            this.maxConnectionLifeTimeUnit = maxConnectionLifeTimeUnit;
        }

        /**
         * @param maxWaitTime
         *        the maxWaitTime to set
         */
        public void setMaxWaitTime(final long maxWaitTime) {
            this.maxWaitTime = maxWaitTime;
        }

        /**
         * @param maxWaitTimeUnit
         *        the maxWaitTimeUnit to set
         */
        public void setMaxWaitTimeUnit(final TimeUnit maxWaitTimeUnit) {
            this.maxWaitTimeUnit = maxWaitTimeUnit;
        }

        /**
         * @param minConnectionsPerHost
         *        the minConnectionsPerHost to set
         */
        public void setMinConnectionsPerHost(final int minConnectionsPerHost) {
            this.minConnectionsPerHost = minConnectionsPerHost;
        }

        /**
         * @param minHeartbeatFrequency
         *        the minHeartbeatFrequency to set
         */
        public void setMinHeartbeatFrequency(final long minHeartbeatFrequency) {
            this.minHeartbeatFrequency = minHeartbeatFrequency;
        }

        /**
         * @param minHeartbeatFrequencyUnit
         *        the minHeartbeatFrequencyUnit to set
         */
        public void setMinHeartbeatFrequencyUnit(final TimeUnit minHeartbeatFrequencyUnit) {
            this.minHeartbeatFrequencyUnit = minHeartbeatFrequencyUnit;
        }

        /**
         * @param password
         *        the password to set
         */
        public void setPassword(final String password) {
            this.password = password;
        }

        /**
         * @param port
         *        the port to set
         */
        public void setPort(final int port) {
            this.port = port;
        }

        /**
         * @param readConcern
         *        the readConcern to set
         */
        public void setReadConcern(final ReadConcern readConcern) {
            this.readConcern = readConcern;
        }

        /**
         * @param serverSelectionTimeout
         *        the serverSelectionTimeout to set
         */
        public void setServerSelectionTimeout(final long serverSelectionTimeout) {
            this.serverSelectionTimeout = serverSelectionTimeout;
        }

        /**
         * @param serverSelectionTimeoutUnit
         *        the serverSelectionTimeoutUnit to set
         */
        public void setServerSelectionTimeoutUnit(final TimeUnit serverSelectionTimeoutUnit) {
            this.serverSelectionTimeoutUnit = serverSelectionTimeoutUnit;
        }

        /**
         * @param socketTimeout
         *        the socketTimeout to set
         */
        public void setSocketTimeout(final long socketTimeout) {
            this.socketTimeout = socketTimeout;
        }

        /**
         * @param socketTimeoutUnit
         *        the socketTimeoutUnit to set
         */
        public void setSocketTimeoutUnit(final TimeUnit socketTimeoutUnit) {
            this.socketTimeoutUnit = socketTimeoutUnit;
        }

        /**
         * @param sslEnabled
         *        the sslEnabled to set
         */
        public void setSslEnabled(final boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
        }

        /**
         * @param threadsAllowedToBlockForConnectionMultiplier
         *        the threadsAllowedToBlockForConnectionMultiplier to set
         */
        public void setThreadsAllowedToBlockForConnectionMultiplier(final int threadsAllowedToBlockForConnectionMultiplier) {
            this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
        }

        /**
         * @param userName
         *        the userName to set
         */
        public void setUserName(final String userName) {
            this.userName = userName;
        }

        /**
         * @param writeConcern
         *        the writeConcern to set
         */
        public void setWriteConcern(final WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
        }
    }

    private final MongoDatabase database;
    private final MongoClient mongo;

    public MongoDBDataStore(final Configuration config) {
        final CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry(),
            CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        final MongoClientOptions options = MongoClientOptions.builder().codecRegistry(pojoCodecRegistry).connectionsPerHost(config.getConnectionsPerHost())
            .connectTimeout((int)config.getConnectTimeoutUnit().toMillis(config.getConnectTimeout()))
            .heartbeatConnectTimeout((int)config.getHeartbeatConnectTimeoutUnit().toMillis(config.getHeartbeatConnectTimeout()))
            .heartbeatFrequency((int)config.getHeartbeatFrequencyUnit().toMillis(config.getHeartbeatFrequency()))
            .heartbeatSocketTimeout((int)config.getHeartbeatSocketTimeoutUnit().toMillis(config.getHeartbeatSocketTimeout()))
            .localThreshold((int)config.getLocalThresholdUnit().toMillis(config.getLocalThreshold()))
            .maxConnectionIdleTime((int)config.getMaxConnectionIdleTimeUnit().toMillis(config.getMaxConnectionIdleTime()))
            .maxConnectionLifeTime((int)config.getMaxConnectionLifeTimeUnit().toMillis(config.getMaxConnectionLifeTime()))
            .maxWaitTime((int)config.getMaxWaitTimeUnit().toMillis(config.getMaxWaitTime())).minConnectionsPerHost(config.getMinConnectionsPerHost())
            .minHeartbeatFrequency((int)config.getMinHeartbeatFrequencyUnit().toMillis(config.getMinHeartbeatFrequency())).readConcern(config.getReadConcern())
            .serverSelectionTimeout((int)config.getServerSelectionTimeoutUnit().toMillis(config.getServerSelectionTimeout()))
            .socketTimeout((int)config.getSocketTimeoutUnit().toMillis(config.getSocketTimeout())).sslEnabled(config.isSslEnabled())
            .threadsAllowedToBlockForConnectionMultiplier(config.getThreadsAllowedToBlockForConnectionMultiplier()).writeConcern(config.getWriteConcern())
            .build();

        final ServerAddress address = new ServerAddress(config.getHost(), config.getPort());

        if(config.getUserName() != null || config.getPassword() != null) {
            final MongoCredential credential = MongoCredential.createCredential(config.getUserName(), config.getDatabase(), config.getPassword().toCharArray());
            mongo = new MongoClient(address, Arrays.asList(new MongoCredential[] {credential}), options);
        } else {
            mongo = new MongoClient(address, options);
        }

        database = mongo.getDatabase(config.getDatabase());
    }
    
    protected <T> MongoCollection<T> getCollection(Class<T> clazz) {
        return database.getCollection(clazz.getCanonicalName(), clazz);
    }

    @Override
    public void close() {
        mongo.close();
    }
}

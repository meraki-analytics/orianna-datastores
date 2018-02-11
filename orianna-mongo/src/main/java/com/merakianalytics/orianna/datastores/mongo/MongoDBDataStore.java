package com.merakianalytics.orianna.datastores.mongo;

import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Sorts.ascending;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.AddOriannaIndexFields;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.merakianalytics.datapipelines.AbstractDataStore;
import com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration.ConnectionPoolConfiguration;
import com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration.SSLConfiguration;
import com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration.ServerConfiguration;
import com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration.SocketConfiguration;
import com.merakianalytics.orianna.types.common.OriannaException;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.async.client.AggregateIterable;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;

public abstract class MongoDBDataStore extends AbstractDataStore implements AutoCloseable {
    public static enum Compressor {
            SNAPPY,
            ZLIB;
    }

    public static class Configuration {
        public static class ConnectionPoolConfiguration {
            private Long maintenanceFrequency, maintenanceInitialDelay, maxConnectionIdleTime, maxConnectionLifeTime, maxWaitTime;
            private TimeUnit maintenanceFrequencyUnit, maintenanceInitialDelayUnit, maxConnectionIdleTimeUnit, maxConnectionLifeTimeUnit, maxWaitTimeUnit;
            private Integer maxSize, maxWaitQueueSize, minSize;

            /**
             * @return the maintenanceFrequency
             */
            public Long getMaintenanceFrequency() {
                return maintenanceFrequency;
            }

            /**
             * @return the maintenanceFrequencyUnit
             */
            public TimeUnit getMaintenanceFrequencyUnit() {
                return maintenanceFrequencyUnit;
            }

            /**
             * @return the maintenanceInitialDelay
             */
            public Long getMaintenanceInitialDelay() {
                return maintenanceInitialDelay;
            }

            /**
             * @return the maintenanceInitialDelayUnit
             */
            public TimeUnit getMaintenanceInitialDelayUnit() {
                return maintenanceInitialDelayUnit;
            }

            /**
             * @return the maxConnectionIdleTime
             */
            public Long getMaxConnectionIdleTime() {
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
            public Long getMaxConnectionLifeTime() {
                return maxConnectionLifeTime;
            }

            /**
             * @return the maxConnectionLifeTimeUnit
             */
            public TimeUnit getMaxConnectionLifeTimeUnit() {
                return maxConnectionLifeTimeUnit;
            }

            /**
             * @return the maxSize
             */
            public Integer getMaxSize() {
                return maxSize;
            }

            /**
             * @return the maxWaitQueueSize
             */
            public Integer getMaxWaitQueueSize() {
                return maxWaitQueueSize;
            }

            /**
             * @return the maxWaitTime
             */
            public Long getMaxWaitTime() {
                return maxWaitTime;
            }

            /**
             * @return the maxWaitTimeUnit
             */
            public TimeUnit getMaxWaitTimeUnit() {
                return maxWaitTimeUnit;
            }

            /**
             * @return the minSize
             */
            public Integer getMinSize() {
                return minSize;
            }

            /**
             * @param maintenanceFrequency
             *        the maintenanceFrequency to set
             */
            public void setMaintenanceFrequency(final Long maintenanceFrequency) {
                this.maintenanceFrequency = maintenanceFrequency;
            }

            /**
             * @param maintenanceFrequencyUnit
             *        the maintenanceFrequencyUnit to set
             */
            public void setMaintenanceFrequencyUnit(final TimeUnit maintenanceFrequencyUnit) {
                this.maintenanceFrequencyUnit = maintenanceFrequencyUnit;
            }

            /**
             * @param maintenanceInitialDelay
             *        the maintenanceInitialDelay to set
             */
            public void setMaintenanceInitialDelay(final Long maintenanceInitialDelay) {
                this.maintenanceInitialDelay = maintenanceInitialDelay;
            }

            /**
             * @param maintenanceInitialDelayUnit
             *        the maintenanceInitialDelayUnit to set
             */
            public void setMaintenanceInitialDelayUnit(final TimeUnit maintenanceInitialDelayUnit) {
                this.maintenanceInitialDelayUnit = maintenanceInitialDelayUnit;
            }

            /**
             * @param maxConnectionIdleTime
             *        the maxConnectionIdleTime to set
             */
            public void setMaxConnectionIdleTime(final Long maxConnectionIdleTime) {
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
            public void setMaxConnectionLifeTime(final Long maxConnectionLifeTime) {
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
             * @param maxSize
             *        the maxSize to set
             */
            public void setMaxSize(final Integer maxSize) {
                this.maxSize = maxSize;
            }

            /**
             * @param maxWaitQueueSize
             *        the maxWaitQueueSize to set
             */
            public void setMaxWaitQueueSize(final Integer maxWaitQueueSize) {
                this.maxWaitQueueSize = maxWaitQueueSize;
            }

            /**
             * @param maxWaitTime
             *        the maxWaitTime to set
             */
            public void setMaxWaitTime(final Long maxWaitTime) {
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
             * @param minSize
             *        the minSize to set
             */
            public void setMinSize(final Integer minSize) {
                this.minSize = minSize;
            }
        }

        public static class HeartbeatConfiguration {
            private Long heartbeatFrequency, minHeartbeatFrequency;
            private TimeUnit heartbeatFrequencyUnit, minHeartbeatFrequencyUnit;

            /**
             * @return the heartbeatFrequency
             */
            public Long getHeartbeatFrequency() {
                return heartbeatFrequency;
            }

            /**
             * @return the heartbeatFrequencyUnit
             */
            public TimeUnit getHeartbeatFrequencyUnit() {
                return heartbeatFrequencyUnit;
            }

            /**
             * @return the minHeartbeatFrequency
             */
            public Long getMinHeartbeatFrequency() {
                return minHeartbeatFrequency;
            }

            /**
             * @return the minHeartbeatFrequencyUnit
             */
            public TimeUnit getMinHeartbeatFrequencyUnit() {
                return minHeartbeatFrequencyUnit;
            }

            /**
             * @param heartbeatFrequency
             *        the heartbeatFrequency to set
             */
            public void setHeartbeatFrequency(final Long heartbeatFrequency) {
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
             * @param minHeartbeatFrequency
             *        the minHeartbeatFrequency to set
             */
            public void setMinHeartbeatFrequency(final Long minHeartbeatFrequency) {
                this.minHeartbeatFrequency = minHeartbeatFrequency;
            }

            /**
             * @param minHeartbeatFrequencyUnit
             *        the minHeartbeatFrequencyUnit to set
             */
            public void setMinHeartbeatFrequencyUnit(final TimeUnit minHeartbeatFrequencyUnit) {
                this.minHeartbeatFrequencyUnit = minHeartbeatFrequencyUnit;
            }
        }

        public static class ServerConfiguration {
            private Long heartbeatFrequency, minHeartbeatFrequency;
            private TimeUnit heartbeatFrequencyUnit, minHeartbeatFrequencyUnit;

            /**
             * @return the heartbeatFrequency
             */
            public Long getHeartbeatFrequency() {
                return heartbeatFrequency;
            }

            /**
             * @return the heartbeatFrequencyUnit
             */
            public TimeUnit getHeartbeatFrequencyUnit() {
                return heartbeatFrequencyUnit;
            }

            /**
             * @return the minHeartbeatFrequency
             */
            public Long getMinHeartbeatFrequency() {
                return minHeartbeatFrequency;
            }

            /**
             * @return the minHeartbeatFrequencyUnit
             */
            public TimeUnit getMinHeartbeatFrequencyUnit() {
                return minHeartbeatFrequencyUnit;
            }

            /**
             * @param heartbeatFrequency
             *        the heartbeatFrequency to set
             */
            public void setHeartbeatFrequency(final Long heartbeatFrequency) {
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
             * @param minHeartbeatFrequency
             *        the minHeartbeatFrequency to set
             */
            public void setMinHeartbeatFrequency(final Long minHeartbeatFrequency) {
                this.minHeartbeatFrequency = minHeartbeatFrequency;
            }

            /**
             * @param minHeartbeatFrequencyUnit
             *        the minHeartbeatFrequencyUnit to set
             */
            public void setMinHeartbeatFrequencyUnit(final TimeUnit minHeartbeatFrequencyUnit) {
                this.minHeartbeatFrequencyUnit = minHeartbeatFrequencyUnit;
            }
        }

        public static class SocketConfiguration {
            private Integer connectTimeout, readTimeout, receiveBufferSize, sendBufferSize;
            private TimeUnit connectTimeoutUnit, readTimeoutUnit;

            /**
             * @return the connectTimeout
             */
            public Integer getConnectTimeout() {
                return connectTimeout;
            }

            /**
             * @return the connectTimeoutUnit
             */
            public TimeUnit getConnectTimeoutUnit() {
                return connectTimeoutUnit;
            }

            /**
             * @return the readTimeout
             */
            public Integer getReadTimeout() {
                return readTimeout;
            }

            /**
             * @return the readTimeoutUnit
             */
            public TimeUnit getReadTimeoutUnit() {
                return readTimeoutUnit;
            }

            /**
             * @return the receiveBufferSize
             */
            public Integer getReceiveBufferSize() {
                return receiveBufferSize;
            }

            /**
             * @return the sendBufferSize
             */
            public Integer getSendBufferSize() {
                return sendBufferSize;
            }

            /**
             * @param connectTimeout
             *        the connectTimeout to set
             */
            public void setConnectTimeout(final Integer connectTimeout) {
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
             * @param readTimeout
             *        the readTimeout to set
             */
            public void setReadTimeout(final Integer readTimeout) {
                this.readTimeout = readTimeout;
            }

            /**
             * @param readTimeoutUnit
             *        the readTimeoutUnit to set
             */
            public void setReadTimeoutUnit(final TimeUnit readTimeoutUnit) {
                this.readTimeoutUnit = readTimeoutUnit;
            }

            /**
             * @param receiveBufferSize
             *        the receiveBufferSize to set
             */
            public void setReceiveBufferSize(final Integer receiveBufferSize) {
                this.receiveBufferSize = receiveBufferSize;
            }

            /**
             * @param sendBufferSize
             *        the sendBufferSize to set
             */
            public void setSendBufferSize(final Integer sendBufferSize) {
                this.sendBufferSize = sendBufferSize;
            }
        }

        public static class SSLConfiguration {
            private Boolean enabled, invalidHostNameallowed;

            /**
             * @return the enabled
             */
            public Boolean getEnabled() {
                return enabled;
            }

            /**
             * @return the invalidHostNameallowed
             */
            public Boolean getInvalidHostNameallowed() {
                return invalidHostNameallowed;
            }

            /**
             * @param enabled
             *        the enabled to set
             */
            public void setEnabled(final Boolean enabled) {
                this.enabled = enabled;
            }

            /**
             * @param invalidHostNameallowed
             *        the invalidHostNameallowed to set
             */
            public void setInvalidHostNameallowed(final Boolean invalidHostNameallowed) {
                this.invalidHostNameallowed = invalidHostNameallowed;
            }
        }

        private Compressor compressor;
        private ConnectionPoolConfiguration connectionPool;
        private String database = "orianna";
        private HeartbeatConfiguration heartbeat;
        private String host = "localhost";
        private Integer port = 27017;
        private ReadConcern readConcern;
        private Boolean retryWrites;
        private ServerConfiguration server;
        private SocketConfiguration socket, heartbeatSocket;
        private SSLConfiguration ssl;
        private String userName, password;
        private WriteConcern writeConcern;

        /**
         * @return the compressor
         */
        public Compressor getCompressor() {
            return compressor;
        }

        /**
         * @return the connectionPool
         */
        public ConnectionPoolConfiguration getConnectionPool() {
            return connectionPool;
        }

        /**
         * @return the database
         */
        public String getDatabase() {
            return database;
        }

        /**
         * @return the heartbeat
         */
        public HeartbeatConfiguration getHeartbeat() {
            return heartbeat;
        }

        /**
         * @return the heartbeatSocket
         */
        public SocketConfiguration getHeartbeatSocket() {
            return heartbeatSocket;
        }

        /**
         * @return the host
         */
        public String getHost() {
            return host;
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
        public Integer getPort() {
            return port;
        }

        /**
         * @return the readConcern
         */
        public ReadConcern getReadConcern() {
            return readConcern;
        }

        /**
         * @return the retryWrites
         */
        public Boolean getRetryWrites() {
            return retryWrites;
        }

        /**
         * @return the server
         */
        public ServerConfiguration getServer() {
            return server;
        }

        /**
         * @return the socket
         */
        public SocketConfiguration getSocket() {
            return socket;
        }

        /**
         * @return the ssl
         */
        public SSLConfiguration getSsl() {
            return ssl;
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
         * @param compressor
         *        the compressor to set
         */
        public void setCompressor(final Compressor compressor) {
            this.compressor = compressor;
        }

        /**
         * @param connectionPool
         *        the connectionPool to set
         */
        public void setConnectionPool(final ConnectionPoolConfiguration connectionPool) {
            this.connectionPool = connectionPool;
        }

        /**
         * @param database
         *        the database to set
         */
        public void setDatabase(final String database) {
            this.database = database;
        }

        /**
         * @param heartbeat
         *        the heartbeat to set
         */
        public void setHeartbeat(final HeartbeatConfiguration heartbeat) {
            this.heartbeat = heartbeat;
        }

        /**
         * @param heartbeatSocket
         *        the heartbeatSocket to set
         */
        public void setHeartbeatSocket(final SocketConfiguration heartbeatSocket) {
            this.heartbeatSocket = heartbeatSocket;
        }

        /**
         * @param host
         *        the host to set
         */
        public void setHost(final String host) {
            this.host = host;
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
        public void setPort(final Integer port) {
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
         * @param retryWrites
         *        the retryWrites to set
         */
        public void setRetryWrites(final Boolean retryWrites) {
            this.retryWrites = retryWrites;
        }

        /**
         * @param server
         *        the server to set
         */
        public void setServer(final ServerConfiguration server) {
            this.server = server;
        }

        /**
         * @param socket
         *        the socket to set
         */
        public void setSocket(final SocketConfiguration socket) {
            this.socket = socket;
        }

        /**
         * @param ssl
         *        the ssl to set
         */
        public void setSsl(final SSLConfiguration ssl) {
            this.ssl = ssl;
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

    protected static class FindQuery {
        public static class Builder {
            private Bson filter;
            private List<? extends BsonValue> order;
            private String orderingField;

            private Builder() {}

            public FindQuery build() {
                if(filter == null || order == null || orderingField == null) {
                    throw new IllegalStateException("Must set filter, order, and orderingField!");
                }

                final FindQuery query = new FindQuery();
                query.filter = filter;
                query.order = order;
                query.orderingField = orderingField;
                return query;
            }

            public Builder filter(final Bson filter) {
                this.filter = filter;
                return this;
            }

            public Builder order(final List<? extends BsonValue> order) {
                this.order = order;
                return this;
            }

            public Builder orderingField(final String orderingField) {
                this.orderingField = orderingField;
                return this;
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        private Bson filter;
        private List<? extends BsonValue> order;
        private String orderingField;

        private FindQuery() {}

        /**
         * @return the filter
         */
        public Bson getFilter() {
            return filter;
        }

        /**
         * @return the order
         */
        public List<? extends BsonValue> getOrder() {
            return order;
        }

        /**
         * @return the orderingField
         */
        public String getOrderingField() {
            return orderingField;
        }
    }

    private static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions().ordered(false);
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBDataStore.class);
    private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);

    protected static Number fromBson(final BsonNumber number) {
        if(number instanceof BsonInt32) {
            return number.intValue();
        } else if(number instanceof BsonInt64) {
            return number.longValue();
        } else {
            return number.doubleValue();
        }
    }

    private static MongoClientSettings fromConfiguration(final Configuration config) {
        final MongoClientSettings.Builder builder = MongoClientSettings.builder();

        final CodecRegistry registry = CodecRegistries.fromRegistries(MongoClients.getDefaultCodecRegistry(),
            CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).conventions(Lists.newArrayList(new AddOriannaIndexFields())).build()));
        builder.codecRegistry(registry);

        if(config.getCompressor() != null) {
            switch(config.getCompressor()) {
                case SNAPPY:
                    builder.compressorList(ImmutableList.of(MongoCompressor.createSnappyCompressor()));
                    break;
                case ZLIB:
                    builder.compressorList(ImmutableList.of(MongoCompressor.createZlibCompressor()));
                    break;
                default:
                    break;
            }
        }

        builder.clusterSettings(ClusterSettings.builder().hosts(Lists.newArrayList(new ServerAddress(config.getHost(), config.getPort())))
            .mode(ClusterConnectionMode.SINGLE).build());

        if(config.getConnectionPool() != null) {
            final ConnectionPoolConfiguration conf = config.getConnectionPool();
            final ConnectionPoolSettings.Builder connectionPool = ConnectionPoolSettings.builder();
            if(conf.getMaintenanceFrequency() != null) {
                connectionPool.maintenanceFrequency(conf.getMaintenanceFrequency(),
                    conf.getMaintenanceFrequencyUnit() == null ? TimeUnit.MILLISECONDS : conf.getMaintenanceFrequencyUnit());
            }
            if(conf.getMaintenanceInitialDelay() != null) {
                connectionPool.maintenanceInitialDelay(conf.getMaintenanceInitialDelay(),
                    conf.getMaintenanceInitialDelayUnit() == null ? TimeUnit.MILLISECONDS : conf.getMaintenanceInitialDelayUnit());
            }
            if(conf.getMaxConnectionIdleTime() != null) {
                connectionPool.maxConnectionIdleTime(conf.getMaxConnectionIdleTime(),
                    conf.getMaxConnectionIdleTimeUnit() == null ? TimeUnit.MILLISECONDS : conf.getMaxConnectionIdleTimeUnit());
            }
            if(conf.getMaxConnectionLifeTime() != null) {
                connectionPool.maxConnectionLifeTime(conf.getMaxConnectionLifeTime(),
                    conf.getMaxConnectionLifeTimeUnit() == null ? TimeUnit.MILLISECONDS : conf.getMaxConnectionLifeTimeUnit());
            }
            if(conf.getMaxSize() != null) {
                connectionPool.maxSize(conf.getMaxSize());
            }
            if(conf.getMaxWaitQueueSize() != null) {
                connectionPool.maxWaitQueueSize(conf.getMaxWaitQueueSize());
            }
            if(conf.getMaxWaitTime() != null) {
                connectionPool.maxWaitTime(conf.getMaxWaitTime(), conf.getMaxWaitTimeUnit() == null ? TimeUnit.MILLISECONDS : conf.getMaxWaitTimeUnit());
            }
            if(conf.getMinSize() != null) {
                connectionPool.minSize(conf.getMinSize());
            }
            builder.connectionPoolSettings(connectionPool.build());
        }

        if(config.getPassword() != null) {
            builder.credential(MongoCredential.createCredential(config.getUserName(), config.getDatabase(), config.getPassword().toCharArray()));
        }

        if(config.getHeartbeatSocket() != null) {
            final SocketConfiguration conf = config.getHeartbeatSocket();
            final SocketSettings.Builder heartbeatSocket = SocketSettings.builder();
            if(conf.getConnectTimeout() != null) {
                heartbeatSocket.connectTimeout(conf.getConnectTimeout(),
                    conf.getConnectTimeoutUnit() == null ? TimeUnit.MILLISECONDS : conf.getConnectTimeoutUnit());
            }
            if(conf.getReadTimeout() != null) {
                heartbeatSocket.readTimeout(conf.getReadTimeout(), conf.getReadTimeoutUnit() == null ? TimeUnit.MILLISECONDS : conf.getReadTimeoutUnit());
            }
            if(conf.getReceiveBufferSize() != null) {
                heartbeatSocket.receiveBufferSize(conf.getReceiveBufferSize());
            }
            if(conf.getSendBufferSize() != null) {
                heartbeatSocket.sendBufferSize(conf.getSendBufferSize());
            }
            builder.heartbeatSocketSettings(heartbeatSocket.build());
        }

        if(config.getReadConcern() != null) {
            builder.readConcern(config.getReadConcern());
        }

        if(config.getRetryWrites() != null) {
            builder.retryWrites(config.getRetryWrites());
        }

        if(config.getServer() != null) {
            final ServerConfiguration conf = config.getServer();
            final ServerSettings.Builder server = ServerSettings.builder();
            if(conf.getHeartbeatFrequency() != null) {
                server.heartbeatFrequency(conf.getHeartbeatFrequency(),
                    conf.getHeartbeatFrequencyUnit() == null ? TimeUnit.MILLISECONDS : conf.getHeartbeatFrequencyUnit());
            }
            if(conf.getMinHeartbeatFrequency() != null) {
                server.minHeartbeatFrequency(conf.getMinHeartbeatFrequency(),
                    conf.getMinHeartbeatFrequencyUnit() == null ? TimeUnit.MILLISECONDS : conf.getMinHeartbeatFrequencyUnit());
            }
            builder.serverSettings(server.build());
        }

        if(config.getSocket() != null) {
            final SocketConfiguration conf = config.getSocket();
            final SocketSettings.Builder socket = SocketSettings.builder();
            if(conf.getConnectTimeout() != null) {
                socket.connectTimeout(conf.getConnectTimeout(), conf.getConnectTimeoutUnit() == null ? TimeUnit.MILLISECONDS : conf.getConnectTimeoutUnit());
            }
            if(conf.getReadTimeout() != null) {
                socket.readTimeout(conf.getReadTimeout(), conf.getReadTimeoutUnit() == null ? TimeUnit.MILLISECONDS : conf.getReadTimeoutUnit());
            }
            if(conf.getReceiveBufferSize() != null) {
                socket.receiveBufferSize(conf.getReceiveBufferSize());
            }
            if(conf.getSendBufferSize() != null) {
                socket.sendBufferSize(conf.getSendBufferSize());
            }
            builder.socketSettings(socket.build());
        }

        if(config.getSsl() != null) {
            final SSLConfiguration conf = config.getSsl();
            final SslSettings.Builder ssl = SslSettings.builder();
            if(conf.getEnabled() != null) {
                ssl.enabled(conf.getEnabled());
            }
            if(conf.getInvalidHostNameallowed() != null) {
                ssl.invalidHostNameAllowed(conf.getInvalidHostNameallowed());
            }
            builder.sslSettings(ssl.build());
        }

        if(config.getWriteConcern() != null) {
            builder.writeConcern(config.getWriteConcern());
        }

        return builder.build();
    }

    protected static BsonNumber toBson(final Number number) {
        if(number instanceof Integer) {
            return new BsonInt32(number.intValue());
        } else if(number instanceof Long) {
            return new BsonInt64(number.longValue());
        } else {
            return new BsonDouble(number.doubleValue());
        }
    }

    private final Map<Class<?>, String> collectionNames = new ConcurrentHashMap<>();
    private final MongoDatabase db;
    private final MongoClient mongo;

    public MongoDBDataStore(final Configuration config) {
        mongo = MongoClients.create(fromConfiguration(config));
        db = mongo.getDatabase(config.getDatabase());
    }

    @Override
    public void close() {
        mongo.close();
    }

    protected <T> FindResultIterator<T> find(final Class<T> clazz) {
        return find(clazz, (Bson)null);
    }

    protected <T> FindResultIterator<T> find(final Class<T> clazz, final Bson filter) {
        final MongoCollection<T> collection = getCollection(clazz);
        final CompletableFuture<FindResultIterator<T>> future = new CompletableFuture<>();
        collection.count((final Long count, final Throwable exception) -> {
            if(exception != null) {
                future.completeExceptionally(exception);
            } else if(0L == count) {
                future.complete(null);
            } else {
                future.complete(new FindResultIterator<>(filter == null ? collection.find() : collection.find(filter), count));
            }
        });
        try {
            return future.get();
        } catch(InterruptedException | ExecutionException e) {
            LOGGER.error("Error on MongoDB query!", e);
            throw new OriannaException("Error on MongoDB query!", e);
        }
    }

    protected <T> FindResultIterator<T> find(final Class<T> clazz, final FindQuery find) {
        final MongoCollection<T> collection = getCollection(clazz);

        if(find == null) {
            return find(clazz, (Bson)null);
        }

        final CompletableFuture<FindResultIterator<T>> future = new CompletableFuture<>();
        collection.count(find.getFilter(), (final Long count, final Throwable exception) -> {
            if(exception != null) {
                future.completeExceptionally(exception);
            } else if(find.getOrder().size() > count) {
                future.complete(null);
            } else {
                final AggregateIterable<T> result = collection.aggregate(Lists.newArrayList(
                    match(find.getFilter()),
                    addFields(new Field<Bson>("__order",
                        new BsonDocument("$indexOfArray",
                            new BsonArray(Lists.newArrayList(new BsonArray(find.getOrder()), new BsonString("$" + find.getOrderingField())))))),
                    sort(ascending("__order"))));

                future.complete(new FindResultIterator<>(result, count));
            }
        });
        try {
            return future.get();
        } catch(InterruptedException | ExecutionException e) {
            LOGGER.error("Error on MongoDB query!", e);
            throw new OriannaException("Error on MongoDB query!", e);
        }
    }

    protected <T> T findFirst(final Class<T> clazz) {
        return findFirst(clazz, null);
    }

    protected <T> T findFirst(final Class<T> clazz, final Bson filter) {
        final MongoCollection<T> collection = getCollection(clazz);
        final FindIterable<T> find = filter == null ? collection.find() : collection.find(filter);

        final CompletableFuture<T> future = new CompletableFuture<>();
        find.first((final T result, final Throwable exception) -> {
            if(exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(result);
            }
        });

        try {
            return future.get();
        } catch(InterruptedException | ExecutionException e) {
            LOGGER.error("Error on MongoDB query!", e);
            throw new OriannaException("Error on MongoDB query!", e);
        }
    }

    protected <T> MongoCollection<T> getCollection(final Class<T> clazz) {
        String name = collectionNames.get(clazz);
        if(name == null) {
            synchronized(collectionNames) {
                name = collectionNames.get(clazz);
                if(name == null) {
                    // We just want the last 2 packages and the type name
                    final String[] parts = clazz.getCanonicalName().split("\\.");
                    name = parts[parts.length - 3] + "." + parts[parts.length - 2] + "." + parts[parts.length - 1];
                    collectionNames.put(clazz, name);
                }
            }
        }

        return db.getCollection(name, clazz);
    }

    protected List<BsonNumber> numbersToBson(final Iterable<Number> numbers) {
        return StreamSupport.stream(numbers.spliterator(), false).map(MongoDBDataStore::toBson).collect(Collectors.toList());
    }

    protected List<BsonString> stringsToBson(final Iterable<String> strings) {
        return StreamSupport.stream(strings.spliterator(), false).map((final String string) -> {
            return new BsonString(string);
        }).collect(Collectors.toList());
    }

    protected <T> void upsert(final Class<T> clazz, final Iterable<T> objects, final Function<T, Bson> filter) {
        final List<WriteModel<T>> writes = StreamSupport.stream(objects.spliterator(), false).map((final T object) -> {
            return new ReplaceOneModel<>(filter.apply(object), object, UPDATE_OPTIONS);
        }).collect(Collectors.toList());

        final MongoCollection<T> collection = getCollection(clazz);
        collection.bulkWrite(writes, BULK_WRITE_OPTIONS, (final BulkWriteResult result, final Throwable exception) -> {
            if(exception != null) {
                LOGGER.error("Error bulk upserting to MongoDB!", exception);
                throw new OriannaException("Error bulk upserting to MongoDB!", exception);
            }
        });
    }

    protected <T> void upsert(final Class<T> clazz, final T object, final Bson filter) {
        final MongoCollection<T> collection = getCollection(clazz);
        collection.replaceOne(filter, object, UPDATE_OPTIONS, (final UpdateResult result, final Throwable exception) -> {
            if(exception != null) {
                LOGGER.error("Error upserting to MongoDB!", exception);
                throw new OriannaException("Error upserting to MongoDB!", exception);
            }
        });
    }
}

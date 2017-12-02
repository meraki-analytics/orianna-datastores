package com.merakianalytics.orianna.datastores.mongo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.merakianalytics.datapipelines.AbstractDataStore;
import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration.ConnectionPoolConfiguration;
import com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration.SSLConfiguration;
import com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration.ServerConfiguration;
import com.merakianalytics.orianna.datastores.mongo.MongoDBDataStore.Configuration.SocketConfiguration;
import com.merakianalytics.orianna.types.common.OriannaException;
import com.mongodb.ConnectionString;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;

public abstract class MongoDBDataStore extends AbstractDataStore implements AutoCloseable {
    public static enum Compressor {
            DEFAULT,
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

        private Compressor compressor = Compressor.DEFAULT;
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
        public SSLConfiguration getSSL() {
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
        public void setSSL(final SSLConfiguration ssl) {
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

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBDataStore.class);

    private static MongoClientSettings fromConfiguration(final Configuration config) {
        final MongoClientSettings.Builder builder = MongoClientSettings.builder();

        final CodecRegistry registry = CodecRegistries.fromRegistries(MongoClients.getDefaultCodecRegistry(),
            CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        builder.codecRegistry(registry);

        if(config.getCompressor() != Compressor.DEFAULT) {
            switch(config.getCompressor()) {
                case DEFAULT:
                    break;
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

        // host, port, username, password, database
        final StringBuilder connectionString = new StringBuilder("mongodb://");
        if(config.getUserName() != null) {
            if(config.getPassword() != null) {
                connectionString.append(config.getUserName() + ":" + config.getPassword() + "@");
            } else {
                connectionString.append(config.getUserName() + "@");
            }
        }
        connectionString.append(config.getHost() + ":" + config.getPort() + "/" + config.getDatabase());

        builder.clusterSettings(ClusterSettings.builder().applyConnectionString(new ConnectionString(connectionString.toString())).build());

        final ConnectionPoolSettings.Builder connectionPool = ConnectionPoolSettings.builder();
        connectionPool.applyConnectionString(new ConnectionString(connectionString.toString()));

        if(config.getConnectionPool() != null) {
            final ConnectionPoolConfiguration conf = config.getConnectionPool();
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
        }
        builder.connectionPoolSettings(connectionPool.build());

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

        if(config.getSSL() != null) {
            final SSLConfiguration conf = config.getSSL();
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

    protected <T> T findFirstSynchronously(final Class<T> clazz) {
        return findFirstSynchronously(clazz, null);
    }

    protected <T> T findFirstSynchronously(final Class<T> clazz, final Bson filter) {
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

    protected <T> CloseableIterator<T> findSynchronously(final Class<T> clazz) {
        return findSynchronously(clazz, null);
    }

    protected <T> CloseableIterator<T> findSynchronously(final Class<T> clazz, final Bson filter) {
        final MongoCollection<T> collection = getCollection(clazz);
        final FindIterable<T> find = filter == null ? collection.find() : collection.find(filter);
        return new SynchronizingCloseableIterator<>(find);
    }

    protected <T> MongoCollection<T> getCollection(final Class<T> clazz) {
        return db.getCollection(clazz.getCanonicalName(), clazz);
    }
}

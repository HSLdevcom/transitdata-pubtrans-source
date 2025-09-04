package fi.hsl.transitdata.pulsarpubtransconnect;

import com.typesafe.config.Config;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static fi.hsl.common.transitdata.TransitdataProperties.KEY_LAST_CACHE_UPDATE_TIMESTAMP;

public class PubtransConnector {

    private static final Logger log = LoggerFactory.getLogger(PubtransConnector.class);

    private Connection connection;
    private long queryStartTime;

    private String queryString;
    private boolean enableCacheCheck;
    private int cacheMaxAgeInMins;
    private int queryTimeoutSecs;

    private PubtransTableHandler handler;
    private JedisExecutor jedisExecutor;
    private Producer<byte[]> producer;

    private PubtransConnector() {
    }

    public static PubtransConnector newInstance(Connection connection,
                                                PulsarApplicationContext context,
                                                PubtransTableType tableType,
                                                JedisExecutor jedisExecutor) throws RuntimeException {
        PubtransConnector connector = new PubtransConnector();

        connector.connection = connection;
        connector.jedisExecutor = jedisExecutor;
        connector.producer = context.getSingleProducer();

        Config config = context.getConfig();
        connector.queryString = queryString(config);
        connector.enableCacheCheck = config.getBoolean("application.enableCacheTimestampCheck");
        connector.cacheMaxAgeInMins = config.getInt("application.cacheMaxAgeInMinutes");
        connector.queryTimeoutSecs = (int) config.getDuration("pubtrans.queryTimeout", TimeUnit.SECONDS);

        log.info("Cache pre-condition enabled: {} with max age {}", connector.enableCacheCheck, connector.cacheMaxAgeInMins);

        log.info("TableType: " + tableType);
        switch (tableType) {
            case ROI_ARRIVAL:
                connector.handler = new ArrivalHandler(context, jedisExecutor);
                break;
            case ROI_DEPARTURE:
                connector.handler = new DepartureHandler(context, jedisExecutor);
                break;
            default:
                throw new IllegalArgumentException("Table type not supported");
        }
        return connector;
    }

    private static String queryString(Config config) {
        String longName = config.getString("pubtrans.longName");
        String shortName = config.getString("pubtrans.shortName");

        return "SELECT * FROM " +
                longName +
                " AS " +
                shortName +
                " WHERE " +
                shortName + ".LastModifiedUTCDateTime > ? " +
                " ORDER BY " +
                shortName + ".LastModifiedUTCDateTime, " +
                shortName + ".IsOnDatedVehicleJourneyId, " +
                shortName + ".JourneyPatternSequenceNumber DESC";
    }

    public boolean checkPrecondition() {
        if (!enableCacheCheck)
            return true;

        String lastUpdate = jedisExecutor.execute(jedis -> jedis.get(KEY_LAST_CACHE_UPDATE_TIMESTAMP));
        log.info("Cache last known update: {}", lastUpdate);
        if (lastUpdate != null) {
            OffsetDateTime dt = OffsetDateTime.parse(lastUpdate, DateTimeFormatter.ISO_DATE_TIME);
            return isCacheValid(dt, cacheMaxAgeInMins);
        } else {
            log.error("Could not find last cache update timestamp from redis");
            return false;
        }
    }

    static boolean isCacheValid(OffsetDateTime lastCacheUpdate, final int cacheMaxAgeInMins) {

        OffsetDateTime now = OffsetDateTime.now();
        //Java8 does not support getting duration as minutes directly.
        final long secondsSinceUpdate = Duration.between(lastCacheUpdate, now).get(ChronoUnit.SECONDS);
        final long minutesSinceUpdate = Math.floorDiv(secondsSinceUpdate, 60);
        log.info("Minutes since last cache update: {}", minutesSinceUpdate);
        log.info("Current time {}, last update {}} => mins from prev update: {}", now, lastCacheUpdate, minutesSinceUpdate);
        return minutesSinceUpdate <= cacheMaxAgeInMins;
    }

    public void queryAndProcessResults() throws SQLException, PulsarClientException {
        queryStartTime = System.currentTimeMillis();
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            statement = connection.prepareStatement(queryString);
            statement.setTimestamp(1, new java.sql.Timestamp(handler.getLastModifiedTimeStamp()));
            statement.setQueryTimeout(queryTimeoutSecs);

            resultSet = statement.executeQuery();

            produceMessages(handler.handleResultSet(resultSet));
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    log.error("Exception while closing result set", e);
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    log.error("Exception while closing statement", e);
                }
            }
        }
    }

    private void produceMessages(Collection<TypedMessageBuilder<byte[]>> messages) throws PulsarClientException {
        if (!producer.isConnected()) {
            throw new PulsarClientException("Producer is not connected");
        }

        for (TypedMessageBuilder<byte[]> msg : messages) {
            msg.sendAsync()
                    .exceptionally(throwable -> {
                        log.error("Failed to send Pulsar message", throwable);
                        return null;
                    });

        }
        //If we want to get Pulsar Exceptions to bubble up into this thread we need to do a sync flush for all pending messages.
        producer.flush();

        log.info("{} messages written. Latest timestamp: {} Total query and processing time: {} ms", messages.size(), handler.getLastModifiedTimeStamp(), System.currentTimeMillis() - this.queryStartTime);
    }
}


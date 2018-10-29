package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

public abstract class PubtransTableHandler {
    static final Logger log = LoggerFactory.getLogger(PubtransTableHandler.class);

    private long lastModifiedTimeStamp;
    Producer<byte[]> producer;
    final TransitdataProperties.ProtobufSchema schema;
    private Jedis jedis;
    private final String timeZone;

    public PubtransTableHandler(PulsarApplicationContext context, TransitdataProperties.ProtobufSchema handlerSchema) {
        lastModifiedTimeStamp = (System.currentTimeMillis() - 5000);
        jedis = context.getJedis();
        producer = context.getProducer();
        timeZone = context.getConfig().getString("pubtrans.timezone");
        schema = handlerSchema;
    }

    public long getLastModifiedTimeStamp() {
        return this.lastModifiedTimeStamp;
    }

    public void setLastModifiedTimeStamp(long ts) {
        this.lastModifiedTimeStamp = ts;
    }

    public Optional<Long> toUtcEpochMs(String localTimestamp) {
        return toUtcEpochMs(localTimestamp, timeZone);
    }

    public static Optional<Long> toUtcEpochMs(String localTimestamp, String zoneId) {
        if (localTimestamp == null || localTimestamp.isEmpty())
            return Optional.empty();

        try {
            LocalDateTime dt = LocalDateTime.parse(localTimestamp.replace(" ", "T")); // Make java.sql.Timestamp ISO compatible
            ZoneId zone = ZoneId.of(zoneId);
            long epochMs = dt.atZone(zone).toInstant().toEpochMilli();
            return Optional.of(epochMs);
        }
        catch (Exception e) {
            log.error("Failed to parse datetime from " + localTimestamp, e);
            return Optional.empty();
        }
    }

    //TODO finetune SQL so that we can use common method to parse most of the fields. now derived classes contain a lot of duplicate code.
    abstract public Queue<TypedMessageBuilder<byte[]>> handleResultSet(ResultSet resultSet) throws SQLException;


    class JourneyInfo {
        String direction;
        String routeName;
        String startTime;
        String operatingDay;

        JourneyInfo(Map<String, String> map) {
            direction = map.get(TransitdataProperties.KEY_DIRECTION);
            routeName = map.get(TransitdataProperties.KEY_ROUTE_NAME);
            startTime = map.get(TransitdataProperties.KEY_START_TIME);
            operatingDay = map.get(TransitdataProperties.KEY_OPERATING_DAY);
            if (!isValid()) {
                //Let's print more info for debugging purposes:
                log.error("JourneyInfo is missing some fields. Content: " + this.toString());
            }
        }

        boolean isValid() {
            return direction != null && routeName != null &&
                   startTime != null && operatingDay != null;
        }

        @Override
        public String toString() {
            return new StringBuilder()
                    .append("direction: ").append(direction)
                    .append(" routeName: ").append(routeName)
                    .append(" startTime: ").append(startTime)
                    .append(" operatingDay: ").append(operatingDay)
                    .toString();
        }
    }

    private Optional<JourneyInfo> getJourneyInfo(long dvjId) {
        String key = TransitdataProperties.REDIS_PREFIX_DVJ + Long.toString(dvjId);
        Optional<Map<String, String>> maybeJourneyInfoMap = Optional.ofNullable(jedis.hgetAll(key));

        return maybeJourneyInfoMap
                .map(JourneyInfo::new)
                .filter(JourneyInfo::isValid);
    }

    private Optional<String> getStopId(long jppId) {
        String key = TransitdataProperties.REDIS_PREFIX_JPP + Long.toString(jppId);
        return Optional.ofNullable(jedis.get(key));
    }

    Optional<TypedMessageBuilder<byte[]>> createMessage(String key, long eventTime, long dvjId, long jppId, byte[] data) {
        Optional<JourneyInfo> maybeJourneyInfo = getJourneyInfo(dvjId);
        if (!maybeJourneyInfo.isPresent()) {
            log.error("Could not find valid JourneyInfo from Redis for dvjId " + dvjId);
        }
        Optional<String> maybeStopId = getStopId(jppId);
        if (!maybeStopId.isPresent()) {
            log.error("Could not find StopId from Redis for dvjId " + dvjId);
        }

        return maybeJourneyInfo.flatMap(journeyInfo ->
                maybeStopId.map(stopId ->
                    producer.newMessage()
                            .key(key)
                            .eventTime(eventTime)
                            .property(TransitdataProperties.KEY_DVJ_ID, Long.toString(dvjId))
                            .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, schema.toString())
                            .property(TransitdataProperties.KEY_DIRECTION, journeyInfo.direction)
                            .property(TransitdataProperties.KEY_ROUTE_NAME, journeyInfo.routeName)
                            .property(TransitdataProperties.KEY_START_TIME, journeyInfo.startTime)
                            .property(TransitdataProperties.KEY_OPERATING_DAY, journeyInfo.operatingDay)
                            .property(TransitdataProperties.KEY_STOP_ID, stopId)
                            .value(data))
                );
    }
}

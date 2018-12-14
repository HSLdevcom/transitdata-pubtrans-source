package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedList;
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

    abstract protected byte[] createPayload(ResultSet resultSet, PubtransTableProtos.Common common) throws SQLException;

    abstract protected String getTimetabledDateTimeColumnName();

    public Queue<TypedMessageBuilder<byte[]>> handleResultSet(ResultSet resultSet) throws SQLException {

        Queue<TypedMessageBuilder<byte[]>> messageBuilderQueue = new LinkedList<>();

        long tempTimeStamp = getLastModifiedTimeStamp();

        while (resultSet.next()) {
            PubtransTableProtos.Common common = parseCommon(resultSet);
            final long eventTimestampUtcMs = common.getLastModifiedUtcDateTimeMs();

            final long delay = System.currentTimeMillis() - eventTimestampUtcMs;
            log.debug("delay is {}", delay);

            final String key = resultSet.getString("IsOnDatedVehicleJourneyId") + resultSet.getString("JourneyPatternSequenceNumber");
            final long dvjId = common.getIsOnDatedVehicleJourneyId();
            final long jppId = common.getIsTargetedAtJourneyPatternPointGid();
            final byte[] data = createPayload(resultSet, common);

            Optional<TypedMessageBuilder<byte[]>> maybeBuilder = createMessage(key, eventTimestampUtcMs, dvjId, jppId, data);
            maybeBuilder.ifPresent(messageBuilderQueue::add);

            //Update latest ts for next round
            if (eventTimestampUtcMs > tempTimeStamp) {
                tempTimeStamp = eventTimestampUtcMs;
            }
        }

        setLastModifiedTimeStamp(tempTimeStamp);

        return messageBuilderQueue;
    }

    protected PubtransTableProtos.Common parseCommon(ResultSet resultSet) throws SQLException {
        PubtransTableProtos.Common.Builder commonBuilder = PubtransTableProtos.Common.newBuilder();

        //We're hardcoding the version number to proto file to ease syncing with changes, however we still need to set it since it's a required field
        commonBuilder.setSchemaVersion(commonBuilder.getSchemaVersion());
        commonBuilder.setId(resultSet.getLong("Id"));
        commonBuilder.setIsOnDatedVehicleJourneyId(resultSet.getLong("IsOnDatedVehicleJourneyId"));
        if (resultSet.getBytes("IsOnMonitoredVehicleJourneyId") != null)
            commonBuilder.setIsOnMonitoredVehicleJourneyId(resultSet.getLong("IsOnMonitoredVehicleJourneyId"));
        commonBuilder.setJourneyPatternSequenceNumber(resultSet.getInt("JourneyPatternSequenceNumber"));
        commonBuilder.setIsTimetabledAtJourneyPatternPointGid(resultSet.getLong("IsTimetabledAtJourneyPatternPointGid"));
        commonBuilder.setVisitCountNumber(resultSet.getInt("VisitCountNumber"));
        if (resultSet.getBytes("IsTargetedAtJourneyPatternPointGid") != null)
            commonBuilder.setIsTargetedAtJourneyPatternPointGid(resultSet.getLong("IsTargetedAtJourneyPatternPointGid"));
        if (resultSet.getBytes("WasObservedAtJourneyPatternPointGid") != null)
            commonBuilder.setWasObservedAtJourneyPatternPointGid(resultSet.getLong("WasObservedAtJourneyPatternPointGid"));
        if (resultSet.getBytes(getTimetabledDateTimeColumnName()) != null)
            toUtcEpochMs(resultSet.getString(getTimetabledDateTimeColumnName())).map(commonBuilder::setTimetabledLatestUtcDateTimeMs);
        if (resultSet.getBytes("TargetDateTime") != null)
            toUtcEpochMs(resultSet.getString("TargetDateTime")).map(commonBuilder::setTargetUtcDateTimeMs);
        if (resultSet.getBytes("EstimatedDateTime") != null)
            toUtcEpochMs(resultSet.getString("EstimatedDateTime")).map(commonBuilder::setEstimatedUtcDateTimeMs);
        if (resultSet.getBytes("ObservedDateTime") != null)
            toUtcEpochMs(resultSet.getString("ObservedDateTime")).map(commonBuilder::setObservedUtcDateTimeMs);
        commonBuilder.setState(resultSet.getLong("State"));
        commonBuilder.setType(resultSet.getInt("Type"));
        commonBuilder.setIsValidYesNo(resultSet.getBoolean("IsValidYesNo"));

        //All other timestamps are in local time but Pubtrans stores this field in UTC timezone
        final long eventTimestampUtcMs = resultSet.getTimestamp("LastModifiedUTCDateTime").getTime();
        commonBuilder.setLastModifiedUtcDateTimeMs(eventTimestampUtcMs);
        return commonBuilder.build();
    }


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
                log.warn("JourneyInfo is missing some fields. Content: " + this.toString());
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
            log.warn("Could not find valid JourneyInfo from Redis for dvjId " + dvjId);
        }
        Optional<String> maybeStopId = getStopId(jppId);
        if (!maybeStopId.isPresent()) {
            log.warn("Could not find StopId from Redis for dvjId " + dvjId);
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

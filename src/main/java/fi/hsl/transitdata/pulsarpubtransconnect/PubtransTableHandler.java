package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public abstract class PubtransTableHandler {
    static final Logger log = LoggerFactory.getLogger(PubtransTableHandler.class);

    private long lastModifiedTimeStamp;
    Producer<byte[]> producer;
    final TransitdataProperties.ProtobufSchema schema;
    private Jedis jedis;
    private final String timeZone;
    private final boolean excludeMetroTrips;

    public PubtransTableHandler(PulsarApplicationContext context, TransitdataProperties.ProtobufSchema handlerSchema) {
        lastModifiedTimeStamp = (System.currentTimeMillis() - 5000);
        jedis = context.getJedis();
        producer = context.getSingleProducer();
        timeZone = context.getConfig().getString("pubtrans.timezone");
        excludeMetroTrips = context.getConfig().getBoolean("application.excludeMetroTrips");
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
    
    abstract protected Map<String, Long> getTableColumnToIdMap(ResultSet resultSet) throws SQLException;

    abstract protected byte[] createPayload(
            PubtransTableProtos.Common common, Map<String, Long>
                    columnToIdMap, PubtransTableProtos.DOITripInfo tripInfo) throws SQLException;

    abstract protected String getTimetabledDateTimeColumnName();

    abstract protected TransitdataSchema getSchema();

    public Collection<TypedMessageBuilder<byte[]>> handleResultSet(ResultSet resultSet, PreparedStatement statement)
            throws SQLException {
        List<TypedMessageBuilder<byte[]>> messageBuilderQueue = new ArrayList<>();

        long tempTimeStamp = getLastModifiedTimeStamp();

        int count = 0;
        int metroTripCount = 0;
        Set<String> metroRouteIds = new HashSet<>();

        while (resultSet.next()) {
            count++;

            PubtransTableProtos.Common common = parseCommon(resultSet);
            final long eventTimestampUtcMs = common.getLastModifiedUtcDateTimeMs();

            final long delay = System.currentTimeMillis() - eventTimestampUtcMs;
            log.debug("Delay between current time and estimate publish time is {} ms", delay);

            final String key = resultSet.getString("IsOnDatedVehicleJourneyId") + resultSet.getString("JourneyPatternSequenceNumber");
            final Map<String, Long> columnToIdMap = getTableColumnToIdMap(resultSet);
            
            PubtransConnector.closeQuery(resultSet, statement);
            
            final long dvjId = common.getIsOnDatedVehicleJourneyId();
            final long scheduledJppId = common.getIsTimetabledAtJourneyPatternPointGid();
            final long targetedJppId = common.getIsTargetedAtJourneyPatternPointGid();

            Optional<PubtransTableProtos.DOITripInfo> maybeTripInfo = getTripInfo(dvjId, scheduledJppId, targetedJppId);
            if (maybeTripInfo.isEmpty()) {
                log.warn("Could not find valid DOITripInfo from Redis for dvjId {}, timetabledJppId {}, targetedJppId {}. Ignoring this update ", dvjId, scheduledJppId, targetedJppId);
            } else {
                PubtransTableProtos.DOITripInfo tripInfo = maybeTripInfo.get();
                
                if (excludeMetroTrips && tripInfo.getRouteId().startsWith("31M")) {
                    metroTripCount++;
                    metroRouteIds.add(tripInfo.getRouteId());
                } else {
                    final byte[] data = createPayload(common, columnToIdMap, tripInfo);
                    TypedMessageBuilder<byte[]> msgBuilder = createMessage(key, eventTimestampUtcMs, dvjId, data, getSchema());
                    messageBuilderQueue.add(msgBuilder);
                }
            }

            //Update latest ts for next round
            if (eventTimestampUtcMs > tempTimeStamp) {
                tempTimeStamp = eventTimestampUtcMs;
            }
        }

        log.info("{} rows processed from the result set. {} rows skipped with metro trips (route ids: {})",
                count, metroTripCount, metroRouteIds);

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

    private Optional<String> getStopId(long jppId) {
        synchronized (jedis) {
            String stopIdKey = TransitdataProperties.REDIS_PREFIX_JPP + jppId;
            return Optional.ofNullable(jedis.get(stopIdKey));
        }
    }

    private Optional<Map<String, String>> getTripInfoFields(long dvjId) {
        synchronized (jedis) {
            String tripInfoKey = TransitdataProperties.REDIS_PREFIX_DVJ + dvjId;
            return Optional.ofNullable(jedis.hgetAll(tripInfoKey));
        }
    }

    protected Optional<PubtransTableProtos.DOITripInfo> getTripInfo(long dvjId, long scheduledJppId, long targetedJppId) {
        try {
            Optional<String> maybeScheduledStopId = getStopId(scheduledJppId);
            Optional<String> maybeTargetedStopId = getStopId(targetedJppId);
            Optional<Map<String, String>> maybeTripInfoMap = getTripInfoFields(dvjId);

            if (maybeScheduledStopId.isPresent() && maybeTripInfoMap.isPresent()) {
                PubtransTableProtos.DOITripInfo.Builder builder = PubtransTableProtos.DOITripInfo.newBuilder();
                
                builder.setStopId(maybeScheduledStopId.get());
                maybeTargetedStopId.ifPresent(builder::setTargetedStopId);

                maybeTripInfoMap.ifPresent(map -> {
                    if (map.containsKey(TransitdataProperties.KEY_DIRECTION))
                        builder.setDirectionId(Integer.parseInt(map.get(TransitdataProperties.KEY_DIRECTION)));
                    if (map.containsKey(TransitdataProperties.KEY_ROUTE_NAME))
                        builder.setRouteId(map.get(TransitdataProperties.KEY_ROUTE_NAME));
                    if (map.containsKey(TransitdataProperties.KEY_START_TIME))
                        builder.setStartTime(map.get(TransitdataProperties.KEY_START_TIME));
                    if (map.containsKey(TransitdataProperties.KEY_OPERATING_DAY))
                        builder.setOperatingDay(map.get(TransitdataProperties.KEY_OPERATING_DAY));
                });
                builder.setDvjId(dvjId);
                return Optional.of(builder.build());
            }
            else {
                log.error("Failed to get data from Redis for dvjId {}, timetabledJppId {}", dvjId, scheduledJppId);
                return Optional.empty();
            }
        }
        catch (Exception e) {
            log.warn("Failed to get Trip Info for dvj-id " + dvjId, e);
            return Optional.empty();
        }
    }

    protected TypedMessageBuilder<byte[]> createMessage(String key, long eventTime, long dvjId, byte[] data, TransitdataSchema schema) {
        return producer.newMessage()
                .key(key)
                .eventTime(eventTime)
                .property(TransitdataProperties.KEY_DVJ_ID, Long.toString(dvjId))
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, schema.schema.toString())
                .property(TransitdataProperties.KEY_SCHEMA_VERSION, Integer.toString(schema.schemaVersion.get()))
                .value(data);
    }
}

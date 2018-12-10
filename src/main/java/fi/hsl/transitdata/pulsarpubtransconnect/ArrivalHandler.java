package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import redis.clients.jedis.Jedis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

public class ArrivalHandler extends PubtransTableHandler {

    public ArrivalHandler(PulsarApplicationContext context) {
        super(context, TransitdataProperties.ProtobufSchema.PubtransRoiArrival);
    }

    @Override
    public Queue<TypedMessageBuilder<byte[]>> handleResultSet(ResultSet resultSet) throws SQLException {

        Queue<TypedMessageBuilder<byte[]>> messageBuilderQueue = new LinkedList<>();

        long tempTimeStamp = getLastModifiedTimeStamp();

        while (resultSet.next()) {
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
            if (resultSet.getBytes("TimetabledLatestDateTime") != null)
                toUtcEpochMs(resultSet.getString("TimetabledLatestDateTime")).map(commonBuilder::setTimetabledLatestUtcDateTimeMs);
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
            PubtransTableProtos.Common common = commonBuilder.build();
            final long delay = System.currentTimeMillis() - eventTimestampUtcMs;
            log.debug("delay is {}", delay);

            PubtransTableProtos.ROIArrival.Builder arrivalBuilder = PubtransTableProtos.ROIArrival.newBuilder();
            arrivalBuilder.setCommon(common);
            PubtransTableProtos.ROIArrival arrival = arrivalBuilder.build();

            final String key = resultSet.getString("IsOnDatedVehicleJourneyId") + resultSet.getString("JourneyPatternSequenceNumber");
            final long dvjId = common.getIsOnDatedVehicleJourneyId();
            final long jppId = common.getIsTargetedAtJourneyPatternPointGid();
            final byte[] data = arrival.toByteArray();

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
}

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
            commonBuilder.setId(resultSet.getLong(1));
            commonBuilder.setIsOnDatedVehicleJourneyId(resultSet.getLong(2));
            if (resultSet.getBytes(3) != null)
                commonBuilder.setIsOnMonitoredVehicleJourneyId(resultSet.getLong(3));
            commonBuilder.setJourneyPatternSequenceNumber(resultSet.getInt(4));
            commonBuilder.setIsTimetabledAtJourneyPatternPointGid(resultSet.getLong(5));
            commonBuilder.setVisitCountNumber(resultSet.getInt(6));
            if (resultSet.getBytes(7) != null)
                commonBuilder.setIsTargetedAtJourneyPatternPointGid(resultSet.getLong(7));
            if (resultSet.getBytes(8) != null)
                commonBuilder.setWasObservedAtJourneyPatternPointGid(resultSet.getLong(8));
            if (resultSet.getBytes(9) != null)
                toUtcEpochMs(resultSet.getString(9)).map(commonBuilder::setTimetabledLatestUtcDateTimeMs);
            if (resultSet.getBytes(10) != null)
                toUtcEpochMs(resultSet.getString(10)).map(commonBuilder::setTargetUtcDateTimeMs);
            if (resultSet.getBytes(11) != null)
                toUtcEpochMs(resultSet.getString(11)).map(commonBuilder::setEstimatedUtcDateTimeMs);
            if (resultSet.getBytes(12) != null)
                toUtcEpochMs(resultSet.getString(12)).map(commonBuilder::setObservedUtcDateTimeMs);
            commonBuilder.setState(resultSet.getLong(13));
            commonBuilder.setType(resultSet.getInt(14));
            commonBuilder.setIsValidYesNo(resultSet.getBoolean(15));

            //All other timestamps are in local time but Pubtrans stores this field in UTC timezone
            final long eventTimestampUtcMs = resultSet.getTimestamp(16).getTime();

            commonBuilder.setLastModifiedUtcDateTimeMs(eventTimestampUtcMs);
            PubtransTableProtos.Common common = commonBuilder.build();

            PubtransTableProtos.ROIArrival.Builder arrivalBuilder = PubtransTableProtos.ROIArrival.newBuilder();
            arrivalBuilder.setCommon(common);
            PubtransTableProtos.ROIArrival arrival = arrivalBuilder.build();

            final String key = resultSet.getString(2) + resultSet.getString(4);
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

package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class DepartureHandler extends PubtransTableHandler {

    public DepartureHandler(Producer producer) {
        super(producer);
    }

    @Override
    public Queue<TypedMessageBuilder> handleResultSet(ResultSet resultSet) throws SQLException {

        Queue<TypedMessageBuilder>  messageBuilderQueue = new LinkedList<>();

        long tempTimeStamp = getLastModifiedTimeStamp();

        while (resultSet.next()) {

            PubtransTableProtos.Common.Builder commonBuilder = PubtransTableProtos.Common.newBuilder();

            //This is the LastModifiedUTCDateTime from the database. It will be used in the actual protobuf message and
            //the Pulsar message eventTime field
            long eventTime = resultSet.getTimestamp(19).getTime();
            if (eventTime > tempTimeStamp) tempTimeStamp = eventTime;

            //We're hardcoding the version number to proto file to ease syncing with changes, however we still need to set it since it's a required field
            commonBuilder.setSchemaVersion(commonBuilder.getSchemaVersion());
            commonBuilder.setId(resultSet.getLong(1));
            commonBuilder.setIsOnDatedVehicleJourneyId(resultSet.getLong(2));
            if (resultSet.getBytes(3) != null) commonBuilder.setIsOnMonitoredVehicleJourneyId(resultSet.getLong(3));
            commonBuilder.setJourneyPatternSequenceNumber(resultSet.getInt(4));
            commonBuilder.setIsTimetabledAtJourneyPatternPointGid(resultSet.getLong(5));
            commonBuilder.setVisitCountNumber(resultSet.getInt(6));
            if (resultSet.getBytes(7) != null) commonBuilder.setIsTargetedAtJourneyPatternPointGid(resultSet.getLong(7));
            if (resultSet.getBytes(8) != null) commonBuilder.setWasObservedAtJourneyPatternPointGid(resultSet.getLong(8));
            if (resultSet.getBytes(12) != null) commonBuilder.setTimetabledLatestDateTime(resultSet.getString(12));
            if (resultSet.getBytes(13) != null) commonBuilder.setTargetDateTime(resultSet.getString(13));
            if (resultSet.getBytes(14) != null) commonBuilder.setEstimatedDateTime(resultSet.getString(14));
            if (resultSet.getBytes(15) != null) commonBuilder.setObservedDateTime(resultSet.getString(15));
            commonBuilder.setState(resultSet.getLong(16));
            commonBuilder.setType(resultSet.getInt(17));
            commonBuilder.setIsValidYesNo(resultSet.getBoolean(18));
            commonBuilder.setLastModifiedUtcDateTime(eventTime);

            PubtransTableProtos.Common common = commonBuilder.build();

            PubtransTableProtos.ROIDeparture.Builder departureBuilder = PubtransTableProtos.ROIDeparture.newBuilder();
            departureBuilder.setCommon(common);
            if (resultSet.getBytes(9) != null) departureBuilder.setHasDestinationDisplayId(resultSet.getLong(9));
            if (resultSet.getBytes(10) != null) departureBuilder.setHasDestinationStopAreaGid(resultSet.getLong(10));
            if (resultSet.getBytes(11) != null) departureBuilder.setHasServiceRequirementId(resultSet.getLong(11));
            PubtransTableProtos.ROIDeparture departure = departureBuilder.build();

            TypedMessageBuilder<byte[]> msgBuilder = producer.newMessage()
                    .key(resultSet.getString(2) + resultSet.getString(4))
                    .eventTime(eventTime)
                    .property("table-name", "roi-departure") //TODO remove, deprecated
                    .property("dvj-id", String.valueOf(common.getIsOnDatedVehicleJourneyId()))
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.PubtransRoiDeparture.toString())

                    .value(departure.toByteArray());

            messageBuilderQueue.add(msgBuilder);

        }

        setLastModifiedTimeStamp(tempTimeStamp);

        return messageBuilderQueue;

    }
}

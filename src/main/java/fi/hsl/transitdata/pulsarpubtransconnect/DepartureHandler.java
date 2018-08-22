package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class DepartureHandler extends PubtransTableHandler {

    private static final Logger log = LoggerFactory.getLogger(DepartureHandler.class);

    public DepartureHandler(Producer producer) {
        super(producer);
    }

    @Override
    public Queue<TypedMessageBuilder> handleResultSet(ResultSet resultSet) throws SQLException {

        Queue<TypedMessageBuilder>  messageBuilderQueue = new LinkedList<>();

        long tempTimeStamp = getLastModifiedTimeStamp();

        while (resultSet.next()) {

            PubtransTableProtos.ROIDeparture.Builder departureBuilder = PubtransTableProtos.ROIDeparture.newBuilder();

            //This is the LastModifiedUTCDateTime from the database. It will be used in the actual protobuf message and
            //the Pulsar message eventTime field
            long eventTime = resultSet.getTimestamp(19).getTime();
            if (eventTime > tempTimeStamp) tempTimeStamp = eventTime;

            departureBuilder.setId(resultSet.getLong(1));
            departureBuilder.setIsOnDatedVehicleJourneyId(resultSet.getLong(2));
            if (resultSet.getBytes(3) != null) departureBuilder.setIsOnMonitoredVehicleJourneyId(resultSet.getLong(3));
            departureBuilder.setJourneyPatternSequenceNumber(resultSet.getInt(4));
            departureBuilder.setIsTimetabledAtJourneyPatternPointGid(resultSet.getLong(5));
            departureBuilder.setVisitCountNumber(resultSet.getInt(6));
            if (resultSet.getBytes(7) != null) departureBuilder.setIsTargetedAtJourneyPatternPointGid(resultSet.getLong(7));
            if (resultSet.getBytes(8) != null) departureBuilder.setWasObservedAtJourneyPatternPointGid(resultSet.getLong(8));
            if (resultSet.getBytes(9) != null) departureBuilder.setHasDestinationDisplayId(resultSet.getLong(9));
            if (resultSet.getBytes(10) != null) departureBuilder.setHasDestinationStopAreaGid(resultSet.getLong(10));
            if (resultSet.getBytes(11) != null) departureBuilder.setHasServiceRequirementId(resultSet.getLong(11));
            if (resultSet.getBytes(12) != null) departureBuilder.setTimetabledLatestDateTime(resultSet.getString(12));
            if (resultSet.getBytes(13) != null) departureBuilder.setTargetDateTime(resultSet.getString(13));
            if (resultSet.getBytes(14) != null) departureBuilder.setEstimatedDateTime(resultSet.getString(14));
            if (resultSet.getBytes(15) != null) departureBuilder.setObservedDateTime(resultSet.getString(15));
            departureBuilder.setState(resultSet.getLong(16));
            departureBuilder.setType(resultSet.getInt(17));
            departureBuilder.setIsValidYesNo(resultSet.getBoolean(18));
            departureBuilder.setLastModifiedUTCDateTime(eventTime);

            PubtransTableProtos.ROIDeparture departure = departureBuilder.build();

            TypedMessageBuilder<byte[]> msgBuilder = producer.newMessage()
                    .key(resultSet.getString(2) + resultSet.getString(4))
                    .eventTime(eventTime)
                    .property("table-name", "roi-departure")
                    .property("dvj-id", String.valueOf(departure.getIsOnDatedVehicleJourneyId()))
                    .value(departure.toByteArray());

            messageBuilderQueue.add(msgBuilder);

        }

        setLastModifiedTimeStamp(tempTimeStamp);

        return messageBuilderQueue;

    }
}

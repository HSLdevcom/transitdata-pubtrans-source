package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.transitdata.proto.PubtransTableProtos;
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

            PubtransTableProtos.ROIBase.Builder baseBuilder = PubtransTableProtos.ROIBase.newBuilder();

            //This is the LastModifiedUTCDateTime from the database. It will be used in the actual protobuf message and
            //the Pulsar message eventTime field
            long eventTime = resultSet.getTimestamp(19).getTime();
            if (eventTime > tempTimeStamp) tempTimeStamp = eventTime;

            baseBuilder.setId(resultSet.getLong(1));
            baseBuilder.setIsOnDatedVehicleJourneyId(resultSet.getLong(2));
            if (resultSet.getBytes(3) != null) baseBuilder.setIsOnMonitoredVehicleJourneyId(resultSet.getLong(3));
            baseBuilder.setJourneyPatternSequenceNumber(resultSet.getInt(4));
            baseBuilder.setIsTimetabledAtJourneyPatternPointGid(resultSet.getLong(5));
            baseBuilder.setVisitCountNumber(resultSet.getInt(6));
            if (resultSet.getBytes(7) != null) baseBuilder.setIsTargetedAtJourneyPatternPointGid(resultSet.getLong(7));
            if (resultSet.getBytes(8) != null) baseBuilder.setWasObservedAtJourneyPatternPointGid(resultSet.getLong(8));
            if (resultSet.getBytes(12) != null) baseBuilder.setTimetabledLatestDateTime(resultSet.getString(12));
            if (resultSet.getBytes(13) != null) baseBuilder.setTargetDateTime(resultSet.getString(13));
            if (resultSet.getBytes(14) != null) baseBuilder.setEstimatedDateTime(resultSet.getString(14));
            if (resultSet.getBytes(15) != null) baseBuilder.setObservedDateTime(resultSet.getString(15));
            baseBuilder.setState(resultSet.getLong(16));
            baseBuilder.setType(resultSet.getInt(17));
            baseBuilder.setIsValidYesNo(resultSet.getBoolean(18));
            baseBuilder.setLastModifiedUTCDateTime(eventTime);

            PubtransTableProtos.ROIBase base = baseBuilder.build();

            PubtransTableProtos.ROIDeparture.Builder departureBuilder = PubtransTableProtos.ROIDeparture.newBuilder();
            departureBuilder.setBase(base);
            if (resultSet.getBytes(9) != null) departureBuilder.setHasDestinationDisplayId(resultSet.getLong(9));
            if (resultSet.getBytes(10) != null) departureBuilder.setHasDestinationStopAreaGid(resultSet.getLong(10));
            if (resultSet.getBytes(11) != null) departureBuilder.setHasServiceRequirementId(resultSet.getLong(11));
            PubtransTableProtos.ROIDeparture departure = departureBuilder.build();

            TypedMessageBuilder<byte[]> msgBuilder = producer.newMessage()
                    .key(resultSet.getString(2) + resultSet.getString(4))
                    .eventTime(eventTime)
                    .property("table-name", "roi-departure")
                    .property("dvj-id", String.valueOf(base.getIsOnDatedVehicleJourneyId()))
                    .value(departure.toByteArray());

            messageBuilderQueue.add(msgBuilder);

        }

        setLastModifiedTimeStamp(tempTimeStamp);

        return messageBuilderQueue;

    }
}

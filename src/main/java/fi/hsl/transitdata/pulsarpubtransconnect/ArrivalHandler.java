package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class ArrivalHandler extends PubtransTableHandler {

    public ArrivalHandler(Producer producer) {
        super(producer);
    }

    @Override
    public Queue<TypedMessageBuilder> handleResultSet(ResultSet resultSet) throws SQLException{

        Queue<TypedMessageBuilder>  messageBuilderQueue = new LinkedList<>();

        long tempTimeStamp = getLastModifiedTimeStamp();

        while (resultSet.next()) {

            PubtransTableProtos.ROIArrival.Builder arrivalBuilder = PubtransTableProtos.ROIArrival.newBuilder();

            //This is the LastModifiedUTCDateTime from the database. It will be used in the actual protobuf message and
            //the Pulsar message eventTime field
            long eventTime = resultSet.getTimestamp(16).getTime();
            if (eventTime > tempTimeStamp) tempTimeStamp = eventTime;

            arrivalBuilder.setId(resultSet.getLong(1));
            arrivalBuilder.setIsOnDatedVehicleJourneyId(resultSet.getLong(2));
            if (resultSet.getBytes(3) != null) arrivalBuilder.setIsOnMonitoredVehicleJourneyId(resultSet.getLong(3));
            arrivalBuilder.setJourneyPatternSequenceNumber(resultSet.getInt(4));
            arrivalBuilder.setIsTimetabledAtJourneyPatternPointGid(resultSet.getLong(5));
            arrivalBuilder.setVisitCountNumber(resultSet.getInt(6));
            if (resultSet.getBytes(7) != null) arrivalBuilder.setIsTargetedAtJourneyPatternPointGid(resultSet.getLong(7));
            if (resultSet.getBytes(8) != null) arrivalBuilder.setWasObservedAtJourneyPatternPointGid(resultSet.getLong(8));
            if (resultSet.getBytes(9) != null) arrivalBuilder.setTimetabledLatestDateTime(resultSet.getString(9));
            if (resultSet.getBytes(10) != null) arrivalBuilder.setTargetDateTime(resultSet.getString(10));
            if (resultSet.getBytes(11) != null) arrivalBuilder.setEstimatedDateTime(resultSet.getString(11));
            if (resultSet.getBytes(12) != null) arrivalBuilder.setObservedDateTime(resultSet.getString(12));
            arrivalBuilder.setState(resultSet.getLong(13));
            arrivalBuilder.setType(resultSet.getInt(14));
            arrivalBuilder.setIsValidYesNo(resultSet.getBoolean(15));
            arrivalBuilder.setLastModifiedUTCDateTime(eventTime);

            PubtransTableProtos.ROIArrival arrival = arrivalBuilder.build();

            TypedMessageBuilder msgBuilder = producer.newMessage()
                    .key(resultSet.getString(2) + resultSet.getString(4))
                    .eventTime(eventTime)
                    .property("table-name", "roi-arrival")
                    .property("dvj-id", String.valueOf(arrival.getIsOnDatedVehicleJourneyId()))
                    .value(arrival.toByteArray());

            messageBuilderQueue.add(msgBuilder);
        }

        setLastModifiedTimeStamp(tempTimeStamp);

        return messageBuilderQueue;

    }
}

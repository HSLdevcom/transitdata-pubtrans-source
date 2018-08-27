package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class ArrivalHandler extends PubtransTableHandler {

    public ArrivalHandler(Producer<byte[]> producer) {
        super(producer);
    }

    @Override
    public Queue<TypedMessageBuilder> handleResultSet(ResultSet resultSet) throws SQLException{

        Queue<TypedMessageBuilder>  messageBuilderQueue = new LinkedList<>();

        long tempTimeStamp = getLastModifiedTimeStamp();

        while (resultSet.next()) {

            PubtransTableProtos.ROIBase.Builder baseBuilder = PubtransTableProtos.ROIBase.newBuilder();

            //This is the LastModifiedUTCDateTime from the database. It will be used in the actual protobuf message and
            //the Pulsar message eventTime field
            long eventTime = resultSet.getTimestamp(16).getTime();
            if (eventTime > tempTimeStamp) tempTimeStamp = eventTime;

            baseBuilder.setId(resultSet.getLong(1));
            baseBuilder.setIsOnDatedVehicleJourneyId(resultSet.getLong(2));
            if (resultSet.getBytes(3) != null) baseBuilder.setIsOnMonitoredVehicleJourneyId(resultSet.getLong(3));
            baseBuilder.setJourneyPatternSequenceNumber(resultSet.getInt(4));
            baseBuilder.setIsTimetabledAtJourneyPatternPointGid(resultSet.getLong(5));
            baseBuilder.setVisitCountNumber(resultSet.getInt(6));
            if (resultSet.getBytes(7) != null) baseBuilder.setIsTargetedAtJourneyPatternPointGid(resultSet.getLong(7));
            if (resultSet.getBytes(8) != null) baseBuilder.setWasObservedAtJourneyPatternPointGid(resultSet.getLong(8));
            if (resultSet.getBytes(9) != null) baseBuilder.setTimetabledLatestDateTime(resultSet.getString(9));
            if (resultSet.getBytes(10) != null) baseBuilder.setTargetDateTime(resultSet.getString(10));
            if (resultSet.getBytes(11) != null) baseBuilder.setEstimatedDateTime(resultSet.getString(11));
            if (resultSet.getBytes(12) != null) baseBuilder.setObservedDateTime(resultSet.getString(12));
            baseBuilder.setState(resultSet.getLong(13));
            baseBuilder.setType(resultSet.getInt(14));
            baseBuilder.setIsValidYesNo(resultSet.getBoolean(15));
            baseBuilder.setLastModifiedUTCDateTime(eventTime);

            PubtransTableProtos.ROIBase base = baseBuilder.build();
            PubtransTableProtos.ROIArrival.Builder arrivalBuilder = PubtransTableProtos.ROIArrival.newBuilder();
            arrivalBuilder.setBase(base);
            PubtransTableProtos.ROIArrival arrival = arrivalBuilder.build();

            TypedMessageBuilder msgBuilder = producer.newMessage()
                    .key(resultSet.getString(2) + resultSet.getString(4))
                    .eventTime(eventTime)
                    .property("table-name", "roi-arrival")
                    .property("dvj-id", String.valueOf(base.getIsOnDatedVehicleJourneyId()))
                    .value(arrival.toByteArray());

            messageBuilderQueue.add(msgBuilder);
        }

        setLastModifiedTimeStamp(tempTimeStamp);

        return messageBuilderQueue;

    }
}

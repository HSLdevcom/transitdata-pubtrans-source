package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
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

            PubtransTableProtos.Common.Builder commonBuilder = PubtransTableProtos.Common.newBuilder();

            //This is the LastModifiedUTCDateTime from the database. It will be used in the actual protobuf message and
            //the Pulsar message eventTime field
            long eventTime = resultSet.getTimestamp(16).getTime();
            if (eventTime > tempTimeStamp) tempTimeStamp = eventTime;

            commonBuilder.setId(resultSet.getLong(1));
            commonBuilder.setIsOnDatedVehicleJourneyId(resultSet.getLong(2));
            if (resultSet.getBytes(3) != null) commonBuilder.setIsOnMonitoredVehicleJourneyId(resultSet.getLong(3));
            commonBuilder.setJourneyPatternSequenceNumber(resultSet.getInt(4));
            commonBuilder.setIsTimetabledAtJourneyPatternPointGid(resultSet.getLong(5));
            commonBuilder.setVisitCountNumber(resultSet.getInt(6));
            if (resultSet.getBytes(7) != null) commonBuilder.setIsTargetedAtJourneyPatternPointGid(resultSet.getLong(7));
            if (resultSet.getBytes(8) != null) commonBuilder.setWasObservedAtJourneyPatternPointGid(resultSet.getLong(8));
            if (resultSet.getBytes(9) != null) commonBuilder.setTimetabledLatestDateTime(resultSet.getString(9));
            if (resultSet.getBytes(10) != null) commonBuilder.setTargetDateTime(resultSet.getString(10));
            if (resultSet.getBytes(11) != null) commonBuilder.setEstimatedDateTime(resultSet.getString(11));
            if (resultSet.getBytes(12) != null) commonBuilder.setObservedDateTime(resultSet.getString(12));
            commonBuilder.setState(resultSet.getLong(13));
            commonBuilder.setType(resultSet.getInt(14));
            commonBuilder.setIsValidYesNo(resultSet.getBoolean(15));
            commonBuilder.setLastModifiedUtcDateTime(eventTime);

            PubtransTableProtos.Common common = commonBuilder.build();
            PubtransTableProtos.ROIArrival.Builder arrivalBuilder = PubtransTableProtos.ROIArrival.newBuilder();
            arrivalBuilder.setCommon(common);
            PubtransTableProtos.ROIArrival arrival = arrivalBuilder.build();

            TypedMessageBuilder msgBuilder = producer.newMessage()
                    .key(resultSet.getString(2) + resultSet.getString(4))
                    .eventTime(eventTime)
                    .property("table-name", "roi-arrival") //TODO remove, deprecated
                    .property("dvj-id", String.valueOf(common.getIsOnDatedVehicleJourneyId()))
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.PubtransRoiArrival.toString())
                    .value(arrival.toByteArray());

            messageBuilderQueue.add(msgBuilder);
        }

        setLastModifiedTimeStamp(tempTimeStamp);

        return messageBuilderQueue;

    }
}

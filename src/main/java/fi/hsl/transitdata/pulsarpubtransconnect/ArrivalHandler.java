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
    protected String getTimetabledDateTimeColumnName() {
        return "TimetabledLatestDateTime";
    }

    @Override
    protected byte[] createPayload(ResultSet resultSet, PubtransTableProtos.Common common) throws SQLException {
        PubtransTableProtos.ROIArrival.Builder arrivalBuilder = PubtransTableProtos.ROIArrival.newBuilder();
        arrivalBuilder.setCommon(common);
        PubtransTableProtos.ROIArrival arrival = arrivalBuilder.build();
        return arrival.toByteArray();
    }

}

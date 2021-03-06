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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

public class DepartureHandler extends PubtransTableHandler {

    static final TransitdataSchema schema;
    static {
        int defaultVersion = PubtransTableProtos.ROIDeparture.newBuilder().getSchemaVersion();
        schema = new TransitdataSchema(TransitdataProperties.ProtobufSchema.PubtransRoiDeparture, Optional.of(defaultVersion));
    }

    public DepartureHandler(PulsarApplicationContext context) {
        super(context, TransitdataProperties.ProtobufSchema.PubtransRoiDeparture);
    }

    @Override
    protected String getTimetabledDateTimeColumnName() {
        return "TimetabledEarliestDateTime";
    }

    @Override
    protected TransitdataSchema getSchema() {
        return schema;
    }

    @Override
    protected byte[] createPayload(ResultSet resultSet, PubtransTableProtos.Common common, PubtransTableProtos.DOITripInfo tripInfo) throws SQLException {
        PubtransTableProtos.ROIDeparture.Builder departureBuilder = PubtransTableProtos.ROIDeparture.newBuilder();
        departureBuilder.setSchemaVersion(departureBuilder.getSchemaVersion());
        departureBuilder.setCommon(common);
        departureBuilder.setTripInfo(tripInfo);
        if (resultSet.getBytes("HasDestinationDisplayId") != null)
            departureBuilder.setHasDestinationDisplayId(resultSet.getLong("HasDestinationDisplayId"));
        if (resultSet.getBytes("HasDestinationStopAreaGid") != null)
            departureBuilder.setHasDestinationStopAreaGid(resultSet.getLong("HasDestinationStopAreaGid"));
        if (resultSet.getBytes("HasServiceRequirementId") != null)
            departureBuilder.setHasServiceRequirementId(resultSet.getLong("HasServiceRequirementId"));
        PubtransTableProtos.ROIDeparture departure = departureBuilder.build();
        return departure.toByteArray();
    }

}

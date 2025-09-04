package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class DepartureHandler extends PubtransTableHandler {

    static final TransitdataSchema schema;

    static {
        int defaultVersion = PubtransTableProtos.ROIDeparture.newBuilder().getSchemaVersion();
        schema = new TransitdataSchema(TransitdataProperties.ProtobufSchema.PubtransRoiDeparture, Optional.of(defaultVersion));
    }

    public DepartureHandler(PulsarApplicationContext context, JedisExecutor jedisExecutor) {
        super(context, jedisExecutor);
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

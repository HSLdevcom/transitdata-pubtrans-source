package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

public class ArrivalHandler extends PubtransTableHandler {

    static final TransitdataSchema schema;
    static {
        int defaultVersion = PubtransTableProtos.ROIArrival.newBuilder().getSchemaVersion();
        schema = new TransitdataSchema(TransitdataProperties.ProtobufSchema.PubtransRoiArrival, Optional.of(defaultVersion));
    }

    public ArrivalHandler(PulsarApplicationContext context) {
        super(context, TransitdataProperties.ProtobufSchema.PubtransRoiArrival);
    }

    @Override
    protected String getTimetabledDateTimeColumnName() {
        return "TimetabledLatestDateTime";
    }

    @Override
    protected TransitdataSchema getSchema() {
        return schema;
    }
    
    @Override
    protected Map<String, Long> getTableColumnToIdMap(ResultSet resultSet) throws SQLException {
        return Map.of();
    }

    @Override
    protected byte[] createPayload(PubtransTableProtos.Common common, Map<String,
            Long> columnToIdMap, PubtransTableProtos.DOITripInfo tripInfo) throws SQLException {
        PubtransTableProtos.ROIArrival.Builder arrivalBuilder = PubtransTableProtos.ROIArrival.newBuilder();
        arrivalBuilder.setSchemaVersion(arrivalBuilder.getSchemaVersion());
        arrivalBuilder.setCommon(common);
        arrivalBuilder.setTripInfo(tripInfo);
        PubtransTableProtos.ROIArrival arrival = arrivalBuilder.build();
        return arrival.toByteArray();
    }

}

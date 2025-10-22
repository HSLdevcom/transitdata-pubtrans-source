package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DepartureHandler extends PubtransTableHandler {

    static final TransitdataSchema schema;
    private static final String COLUMN_HAS_DESTINATION_DISPLAY_ID = "HasDestinationDisplayId";
    private static final String COLUMN_HAS_DESTINATION_STOP_AREA_GID = "HasDestinationStopAreaGid";
    private static final String COLUMN_HAS_SERVICE_REQUIREMENT_ID = "HasServiceRequirementId";
    static {
        int defaultVersion = PubtransTableProtos.ROIDeparture.newBuilder().getSchemaVersion();
        schema = new TransitdataSchema(TransitdataProperties.ProtobufSchema.PubtransRoiDeparture,
                Optional.of(defaultVersion));
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
    protected Map<String, Long> getTableColumnToIdMap(ResultSet resultSet) throws SQLException {
        Map<String, Long> columnToIdMap = new HashMap<>();
        if (resultSet.getBytes(COLUMN_HAS_DESTINATION_DISPLAY_ID) != null) {
            columnToIdMap.put(COLUMN_HAS_DESTINATION_DISPLAY_ID, resultSet.getLong(COLUMN_HAS_DESTINATION_DISPLAY_ID));
        }
        if (resultSet.getBytes(COLUMN_HAS_DESTINATION_STOP_AREA_GID) != null) {
            columnToIdMap.put(COLUMN_HAS_DESTINATION_STOP_AREA_GID,
                    resultSet.getLong(COLUMN_HAS_DESTINATION_STOP_AREA_GID));
        }
        if (resultSet.getBytes(COLUMN_HAS_SERVICE_REQUIREMENT_ID) != null) {
            columnToIdMap.put(COLUMN_HAS_SERVICE_REQUIREMENT_ID, resultSet.getLong(COLUMN_HAS_SERVICE_REQUIREMENT_ID));
        }
        return columnToIdMap;
    }

    @Override
    protected byte[] createPayload(PubtransTableProtos.Common common, Map<String, Long> columnToIdMap,
            PubtransTableProtos.DOITripInfo tripInfo) throws SQLException {
        PubtransTableProtos.ROIDeparture.Builder departureBuilder = PubtransTableProtos.ROIDeparture.newBuilder();
        departureBuilder.setSchemaVersion(departureBuilder.getSchemaVersion());
        departureBuilder.setCommon(common);
        departureBuilder.setTripInfo(tripInfo);
        if (columnToIdMap.containsKey(COLUMN_HAS_DESTINATION_DISPLAY_ID))
            departureBuilder.setHasDestinationDisplayId(columnToIdMap.get(COLUMN_HAS_DESTINATION_DISPLAY_ID));
        if (columnToIdMap.containsKey(COLUMN_HAS_DESTINATION_STOP_AREA_GID))
            departureBuilder.setHasDestinationStopAreaGid(columnToIdMap.get(COLUMN_HAS_DESTINATION_STOP_AREA_GID));
        if (columnToIdMap.containsKey(COLUMN_HAS_SERVICE_REQUIREMENT_ID))
            departureBuilder.setHasServiceRequirementId(columnToIdMap.get(COLUMN_HAS_SERVICE_REQUIREMENT_ID));
        PubtransTableProtos.ROIDeparture departure = departureBuilder.build();
        return departure.toByteArray();
    }

}

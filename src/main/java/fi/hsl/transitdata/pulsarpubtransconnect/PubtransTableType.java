package fi.hsl.transitdata.pulsarpubtransconnect;

public enum PubtransTableType {
    ROI_ARRIVAL,
    ROI_DEPARTURE;

    public String toString() {
        switch (this) {
            case ROI_ARRIVAL: return "ptroiarrival";
            case ROI_DEPARTURE: return "ptroideparture";
        }
        return "";
    }

    public static PubtransTableType fromString(String tableName) {
        if ("ptroiarrival".equals(tableName))
            return ROI_ARRIVAL;
        else if ("ptroideparture".equals(tableName))
            return ROI_DEPARTURE;
        return null;
    }
}

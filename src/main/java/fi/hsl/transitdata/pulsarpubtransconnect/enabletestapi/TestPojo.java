package fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TestPojo {
    private Long id;
    private Long IsOnDatedVehicleJourneyId;
    private Long IsOnMonitoredVehicleJourneyId;
    private int JourneyPatternSequenceNumber;
    private Long IsTimetabledAtJourneyPatternPointGid;
    private int VisitCountNumber;
    private Long IsTargetedAtJourneyPatternPointGid;
    private Long WasObservedAtJourneyPatternPointGid;
    private String getTimetabledDateTimeColumnName;
    private String TargetDateTime;
    private String EstimatedDateTime;
    private String ObservedDateTime;
    private Long State;
    private Long Type;
    private boolean IsValidYesNo;
    private String LastModifiedUTCDateTime;
}

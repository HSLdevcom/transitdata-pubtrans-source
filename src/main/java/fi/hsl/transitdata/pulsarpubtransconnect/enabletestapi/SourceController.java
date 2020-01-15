package fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("dev")
@RequestMapping("/api/v1/test")
public class SourceController {
    @Autowired
    private MockPubTransConnector mockPubTransConnector;

    @RequestMapping("/postrow")
    public String postSource(TestPojo testPojo) {
        return mockPubTransConnector.queryAndProcessResults(testPojo);
    }
}

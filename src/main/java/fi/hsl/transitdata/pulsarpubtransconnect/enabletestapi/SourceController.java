package fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi;

import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("dev")
@RequestMapping("/api/v1/test")
public class SourceController {
    @Autowired
    private MockPubTransConnector mockPubTransConnector;

    @RequestMapping("/postrow")
    public ResponseEntity<String> postSource(TestPojo testPojo) throws PulsarClientException {
        mockPubTransConnector.queryAndProcessResults(testPojo);
        return new ResponseEntity<>("Message sent", HttpStatus.OK);
    }
}

package fi.hsl.transitdata.pulsarpubtransconnect.config;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.pulsarpubtransconnect.PubtransTableType;
import fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi.AbstractPubtransConnector;
import fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi.MockPubTransConnector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.TimeZone;

@Component
@Slf4j
@Profile("dev")
public class TestBeans {
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @Bean
    private Config config() {
        String table = ConfigUtils.getEnvOrThrow("PT_TABLE");
        PubtransTableType type = PubtransTableType.fromString(table);
        Config config = null;
        if (type == PubtransTableType.ROI_ARRIVAL) {
            config = ConfigParser.createConfig("arrival.conf");
        } else if (type == PubtransTableType.ROI_DEPARTURE) {
            config = ConfigParser.createConfig("departure.conf");
        } else {
            log.error("Failed to get table name from PT_TABLE-env variable, exiting application");
            System.exit(1);
        }
        return config;
    }

    @Bean
    private PulsarApplication pulsarApplication(Config config) throws Exception {
        return PulsarApplication.newInstance(config);
    }

    @Bean
    private PulsarApplicationContext pulsarApplicationContext(PulsarApplication pulsarApplication) {
        return pulsarApplication.getContext();
    }

    @Bean
    private AbstractPubtransConnector pubtransConnector(PulsarApplicationContext context) throws Exception {
        String table = ConfigUtils.getEnvOrThrow("PT_TABLE");
        PubtransTableType type = PubtransTableType.fromString(table);
        return new MockPubTransConnector(type);
    }
}

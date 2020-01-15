package fi.hsl.transitdata.pulsarpubtransconnect.config;

import org.springframework.context.annotation.Profile;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@EnableWebMvc
@Profile("dev")
public class EnableWebMVC {
}

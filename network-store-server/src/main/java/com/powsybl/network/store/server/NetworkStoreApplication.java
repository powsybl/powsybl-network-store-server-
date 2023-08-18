package com.powsybl.network.store.server;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.powsybl.ws.commons.Utils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication
public class NetworkStoreApplication {

    public static void main(String[] args) {
        Utils.initProperties();
        SpringApplication.run(NetworkStoreApplication.class, args);
    }

    @Bean
    public Module module() {
        return new JodaModule();
    }
}

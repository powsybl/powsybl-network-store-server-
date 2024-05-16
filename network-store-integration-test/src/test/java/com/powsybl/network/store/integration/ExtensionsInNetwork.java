package com.powsybl.network.store.integration;

import com.powsybl.commons.datasource.ReadOnlyDataSource;
import com.powsybl.commons.datasource.ResourceDataSource;
import com.powsybl.commons.datasource.ResourceSet;
import com.powsybl.iidm.network.Network;
import com.powsybl.network.store.client.NetworkStoreService;
import com.powsybl.network.store.client.PreloadingStrategy;
import com.powsybl.network.store.client.RestClient;
import com.powsybl.network.store.client.RestClientImpl;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextHierarchy({
        @ContextConfiguration(classes = {NetworkStoreApplication.class, NetworkStoreService.class})
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class ExtensionsInNetwork {

    @LocalServerPort
    private int randomServerPort;

    private String getBaseUrl() {
        return "http://localhost:" + randomServerPort + "/";
    }

    private NetworkStoreService createNetworkStoreService(RestClientMetrics metrics) {
        RestClient restClient = new RestClientImpl(getBaseUrl());
        if (metrics != null) {
            restClient = new TestRestClient(restClient, metrics);
        }
        return new NetworkStoreService(restClient, PreloadingStrategy.NONE);
    }

    private ResourceDataSource getResource(String fileName, String path) {
        return new ResourceDataSource(FilenameUtils.getBaseName(fileName),
                new ResourceSet(FilenameUtils.getPath(path),
                        FilenameUtils.getName(fileName)));
    }


    @Test
    public void cgmesExtensionsTestGet() {
        try (NetworkStoreService service = createNetworkStoreService(null)) {
            // import new network in the store
            String filePath = "/network_test1_cgmes_metadata_models.xml";
            ReadOnlyDataSource dataSource = getResource(filePath, filePath);
            Network network = service.importNetwork(dataSource);
            assertNotNull(network.getExtensionByName("cgmesMetadataModels"));

            // get network from the store
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());
            Network readNetwork = service.getNetwork(networkIds.keySet().stream().findFirst().get());
            assertNotNull(readNetwork.getExtensionByName("cgmesMetadataModels"));
        }
    }

    @Test
    public void cgmesExtensionsTestStoreOnly() {
        try (NetworkStoreService service = createNetworkStoreService(null)) {
            // import new network in the store
            String filePath = "/network_test1_cgmes_metadata_models.xml";
            ReadOnlyDataSource dataSource = getResource(filePath, filePath);
            Network network = service.importNetwork(dataSource);
            assertNotNull(network.getExtensionByName("cgmesMetadataModels"));
        }
    }
}
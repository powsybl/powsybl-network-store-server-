/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.integration;

import com.powsybl.network.store.client.RestClient;
import com.powsybl.network.store.model.AbstractTopLevelDocument;
import com.powsybl.network.store.model.Attributes;
import com.powsybl.network.store.model.Resource;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class TestRestClient extends AbstractForwardingRestClient {

    private final RestClientMetrics metrics;

    public TestRestClient(RestClient delegate, RestClientMetrics metrics) {
        super(delegate);
        this.metrics = Objects.requireNonNull(metrics);
    }

    @Override
    public <T, D extends AbstractTopLevelDocument<T>> Optional<T> getOne(String target, String url, ParameterizedTypeReference<D> parameterizedTypeReference, Object... uriVariables) {
        metrics.oneGetterCallCount++;
        return super.getOne(target, url, parameterizedTypeReference, uriVariables);
    }

    @Override
    public <T, D extends AbstractTopLevelDocument<T>> List<T> getAll(String target, String url, ParameterizedTypeReference<D> parameterizedTypeReference, Object... uriVariables) {
        metrics.allGetterCallCount++;
        return super.getAll(target, url, parameterizedTypeReference, uriVariables);
    }

    @Override
    public <T extends Attributes> void updateAll(String url, List<Resource<T>> resources, Object... uriVariables) {
        metrics.updatedUrls.add(UriComponentsBuilder.fromUriString(url).buildAndExpand(uriVariables).toString());
        super.updateAll(url, resources, uriVariables);
    }
}

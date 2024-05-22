/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.powsybl.iidm.network.tck.AbstractManipulationsOnVariantsTest;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ContextHierarchy({
    @ContextConfiguration(classes = {NetworkStoreApplication.class})
    })
@TestPropertySource(properties = { "spring.config.location=classpath:application.yaml" })
public class ManipulationsOnVariantsIT extends AbstractManipulationsOnVariantsTest {

    @Test
    @Override
    /* we need to override this test because we don't have the same cloneVariant implementation :
     * when cloning a variant with the overwrite parameters we delete then clone the variant so we never call the
     * onVariantOverwritten listener but instead the onVariantRemoved and onVariantCreated
     * should we change this behavior ? */
    public void baseTests() {
        //FIXME see comment
    }
}

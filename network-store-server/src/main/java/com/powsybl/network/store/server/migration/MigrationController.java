/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.migration;

import com.powsybl.network.store.model.*;
import com.powsybl.network.store.server.NetworkStoreRepository;
import com.powsybl.network.store.server.migration.v211limits.V211LimitsMigration;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@RestController
@RequestMapping(value = "/" + NetworkStoreApi.VERSION + "/migration")
@Tag(name = "Network store migration")
public class MigrationController {

    @Autowired
    private NetworkStoreRepository repository;

    @PutMapping(value = "/v211limits/{networkId}/{variantNum}")
    @Operation(summary = "Migrate limits of a network")
    @ApiResponses(@ApiResponse(responseCode = "200", description = "Successfully migrated limits from V2.11.0 to new model"))
    public ResponseEntity<Void> migrateV211Limits(@Parameter(description = "Network ID", required = true) @PathVariable("networkId") UUID networkId,
                                                  @Parameter(description = "Variant num", required = true) @PathVariable("variantNum") int variantNum) {
        V211LimitsMigration.migrateV211Limits(repository, networkId, variantNum);
        return ResponseEntity.ok().build();
    }
}

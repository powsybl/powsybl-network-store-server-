package com.powsybl.network.store.server.utils;

import com.powsybl.network.store.model.ExtensionAttributes;
import lombok.Builder;

@Builder
public class NonPersistedActivePowerControlAttributes implements ExtensionAttributes {
    @Override
    public boolean isPersisted() {
        return false;
    }

}

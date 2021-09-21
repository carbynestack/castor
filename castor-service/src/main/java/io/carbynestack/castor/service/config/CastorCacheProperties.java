/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.config;

import com.google.common.collect.Sets;
import io.carbynestack.castor.common.entities.TupleType;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "carbynestack.castor.cache")
@Component
@Data
@Accessors(chain = true)
public class CastorCacheProperties {

  private String reservationStore;
  private String consumptionStorePrefix;

  private long telemetryInterval;
  private long telemetryTtl;

  private String host;
  private int port;

  public Set<String> getCacheNames() {
    List<String> cacheNames = new ArrayList<>();
    cacheNames.add(getReservationStore());
    for (TupleType tupleType : TupleType.values()) {
      cacheNames.add(getConsumptionStorePrefix() + tupleType.toString());
    }
    return Sets.newLinkedHashSet(cacheNames);
  }
}

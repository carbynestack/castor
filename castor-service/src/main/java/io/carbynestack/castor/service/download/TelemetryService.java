/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import io.carbynestack.castor.common.entities.TelemetryData;
import io.carbynestack.castor.common.entities.TupleMetric;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.service.persistence.cache.ConsumptionCachingService;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor(onConstructor_ = @Autowired)
public class TelemetryService {

  private TupleChunkFragmentStorageService tupleChunkFragmentStorageService;
  private ConsumptionCachingService consumptionCachingService;

  public TelemetryData getTelemetryDataForInterval(Duration interval) {
    List<TupleMetric> metrics = new ArrayList<>();
    for (TupleType type : TupleType.values()) {
      TupleMetric metric =
          TupleMetric.of(
              tupleChunkFragmentStorageService.getAvailableTuples(type),
              getConsumptionRateForType(interval, type),
              type);
      metrics.add(metric);
    }
    return new TelemetryData(metrics, interval.toMillis());
  }

  private int getConsumptionRateForType(Duration interval, TupleType type) {
    long timeSince = new Date().getTime() - interval.toMillis();
    long consumptionSince = consumptionCachingService.getConsumptionForTupleType(timeSince, type);
    return Math.round(consumptionSince / (float) interval.getSeconds());
  }
}

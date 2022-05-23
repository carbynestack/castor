/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import static io.carbynestack.castor.common.entities.TupleType.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.carbynestack.castor.common.entities.TelemetryData;
import io.carbynestack.castor.common.entities.TupleMetric;
import io.carbynestack.castor.service.persistence.cache.ConsumptionCachingService;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TelemetryServiceTest {
  @Mock private TupleChunkFragmentStorageService fragmentStorageServiceMock;
  @Mock private ConsumptionCachingService consumptionCachingServiceMock;

  @InjectMocks private TelemetryService telemetryService;

  @Test
  public void
      givenSuccessfulRequest_whenGetTelemetryDataForInterval_thenReturnExpectedTelemetryData() {
    Duration interval = Duration.ofSeconds(42);
    List<TupleMetric> expectedMetrics =
        Arrays.asList(
            TupleMetric.of(1, 2, BIT_GFP),
            TupleMetric.of(2, 4, BIT_GF2N),
            TupleMetric.of(3, 8, INPUT_MASK_GFP),
            TupleMetric.of(4, 16, INPUT_MASK_GF2N),
            TupleMetric.of(5, 32, INVERSE_TUPLE_GFP),
            TupleMetric.of(6, 64, INVERSE_TUPLE_GF2N),
            TupleMetric.of(7, 128, SQUARE_TUPLE_GFP),
            TupleMetric.of(8, 256, SQUARE_TUPLE_GF2N),
            TupleMetric.of(9, 512, MULTIPLICATION_TRIPLE_GFP),
            TupleMetric.of(10, 1024, MULTIPLICATION_TRIPLE_GF2N));
    expectedMetrics.forEach(
        m -> {
          when(fragmentStorageServiceMock.getAvailableTuples(m.getType()))
              .thenReturn(m.getAvailable());
          when(consumptionCachingServiceMock.getConsumptionForTupleType(anyLong(), eq(m.getType())))
              .thenReturn(m.getConsumptionRate() * interval.getSeconds());
        });
    TelemetryData actualTelemetryData = telemetryService.getTelemetryDataForInterval(interval);
    assertEquals(interval.toMillis(), actualTelemetryData.getInterval());
    assertEquals(expectedMetrics, actualTelemetryData.getMetrics());
  }
}

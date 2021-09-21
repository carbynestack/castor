/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import lombok.*;

@Data
@Setter(AccessLevel.NONE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor
public class TelemetryData implements Serializable {
  private static final long serialVersionUID = -2657388431163568882L;

  public static final Duration DEFAULT_REQUEST_INTERVAL = Duration.of(60, ChronoUnit.SECONDS);

  private List<TupleMetric> metrics;
  /** interval in milliseconds */
  private long interval;
}

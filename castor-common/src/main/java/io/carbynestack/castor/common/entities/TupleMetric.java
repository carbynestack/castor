/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import java.io.Serializable;
import lombok.*;

/**
 * {@link TupleMetric} provides metrics for a given {@link TupleType} like the number of {@link
 * Tuple} that were available at the time of request, or how many {@link Tuple}s of this type have
 * been consumed by clients or internal services within a defined timeframe.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(staticName = "of")
@Data
@Setter(value = AccessLevel.NONE)
public class TupleMetric implements Serializable {
  private static final long serialVersionUID = -288015321240466836L;

  /** The number of available {@link Tuple}s for the given {@link #type}. */
  private long available;
  /** The consumption rate for the given {@link #type TupleType} and the requested timeframe. */
  private long consumptionRate;
  /** The {@link TupleType} these {@link TupleMetric}s belong to */
  private TupleType type;
}

/*
 * Copyright (c) 2023 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class DedicatedTransactionServiceTest {
  @Test
  void givenSupplier_whenRunAsNewTransaction_thenCallSupplier() {
    Supplier supplierMock = mock(Supplier.class);
    new DedicatedTransactionService().runAsNewTransaction(supplierMock);

    verify(supplierMock).get();
  }
}

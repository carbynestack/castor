/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import java.util.function.Supplier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@ConditionalOnProperty(value = "carbynestack.castor.master", havingValue = "true")
public class DedicatedTransactionService {

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public <T> T runAsNewTransaction(Supplier<T> supplier) {
    return supplier.get();
  }
}

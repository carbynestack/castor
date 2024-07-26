/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.rest;

import static io.carbynestack.castor.common.rest.CastorRestApiEndpoints.*;
import static org.springframework.util.Assert.isTrue;

import io.carbynestack.castor.common.entities.TupleList;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.service.download.TuplesDownloadService;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = INTRA_VCP_OPERATIONS_SEGMENT + TUPLES_ENDPOINT)
@AllArgsConstructor(onConstructor_ = @Autowired)
public class TuplesController {
  private final TuplesDownloadService tuplesDownloadService;

  /**
   * @param type Defines which type of tuples are requested
   * @param count Defines how many multiplication triples are requested
   * @param requestId This identifies the request. All services have to receive the same id for the
   *     same tuples.
   */
  @GetMapping
  public ResponseEntity<byte[]> getTuples(
      @RequestParam(value = DOWNLOAD_TUPLE_TYPE_PARAMETER) String type,
      @RequestParam(value = DOWNLOAD_COUNT_PARAMETER) long count,
      @RequestParam(value = DOWNLOAD_REQUEST_ID_PARAMETER) UUID requestId) {
    Assert.notNull(requestId, "Request identifier must not be omitted");
    isTrue(count > 0, "The number of requested Multiplication Triples has to be 1 or greater.");
    TupleType tupleType = TupleType.valueOf(type);
    return new ResponseEntity<>(
        tuplesDownloadService.getTupleList(
            tupleType.getTupleCls(), tupleType.getField(), count, requestId),
        HttpStatus.OK);
  }
}

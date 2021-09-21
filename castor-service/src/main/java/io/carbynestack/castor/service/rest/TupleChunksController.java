/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.rest;

import static io.carbynestack.castor.common.rest.CastorRestApiEndpoints.*;

import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataEntity;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataStorageService;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = INTRA_VCP_OPERATIONS_SEGMENT + ACTIVATE_TUPLE_CHUNK_ENDPOINT)
@AllArgsConstructor(onConstructor_ = @Autowired)
public class TupleChunksController {
  private final TupleChunkMetaDataStorageService tupleChunkMetaDataStorageService;

  /**
   * @param chunkId Unique identifier of the Tuple Chunk.
   * @return
   */
  @PutMapping(path = "/{" + TUPLE_CHUNK_ID_PARAMETER + "}")
  public ResponseEntity<TupleChunkMetaDataEntity> activateTupleChunk(
      @PathVariable(value = TUPLE_CHUNK_ID_PARAMETER) UUID chunkId) {
    Assert.notNull(chunkId, "Chunk identifier must not be omitted");
    return new ResponseEntity<>(
        this.tupleChunkMetaDataStorageService.activateTupleChunk(chunkId), HttpStatus.OK);
  }
}

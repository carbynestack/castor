/*
 * Copyright (c) 2023 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

class TupleListTest {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @SneakyThrows
  @Test
  void givenValidInputStream_whenCreateFromStream_thenReturnExpectedTupleList() {
    byte[][] tupleValueData =
        new byte[][] {
          RandomUtils.nextBytes(Field.GFP.getElementSize()),
          RandomUtils.nextBytes(Field.GFP.getElementSize()),
          RandomUtils.nextBytes(Field.GFP.getElementSize()),
          RandomUtils.nextBytes(Field.GFP.getElementSize())
        };

    Bit<Field.Gfp> expectedTuple1 =
        new Bit<>(Field.GFP, new Share(tupleValueData[0], tupleValueData[1]));
    Bit<Field.Gfp> expectedTuple2 =
        new Bit<>(Field.GFP, new Share(tupleValueData[2], tupleValueData[3]));
    byte[] tupleData = Arrays.stream(tupleValueData).reduce(ArrayUtils::addAll).get();
    TupleList actualTupleList =
        TupleList.fromStream(
            TupleType.BIT_GFP.getTupleCls(),
            TupleType.BIT_GFP.getField(),
            new ByteArrayInputStream(tupleData),
            tupleData.length);
    assertThat((TupleList<Bit<Field.Gfp>, Field.Gfp>) actualTupleList)
        .containsExactlyElementsOf(Lists.list(expectedTuple1, expectedTuple2));
  }

  @Test
  void givenJsonDoesNotContainRequiredMacValue_whenDeserializeFromJson_thenThrowException() {
    String missingMacData =
        "{\"tupleCls\":\"InputMask\",\"field\":{\"@type\":\"Gfp\",\"name\":\"gfp\",\"elementSize\":128},\"tuples\":"
            + "[{\"@type\":\"InputMask\",\"field\":{\"@type\":\"Gfp\",\"name\":\"gfp\",\"elementSize\":128},\"shares\":[{\"value\":\"DuLIdi2fllkbdinOZKz0z7ZUbqJ5cWM18lp/csHjggMCXleMA5W5GRnEJ8QFTrDO++nm0XPWWQIiZwtT6/keSqqYwPA1EysyZbv8dPNhLWO6VxItSfzJO0hmaIdRnQkcXHKr0Fey0fS/p+n7KCwSwmo7mqEGjwmvFosaRffS+ro=\",\"mac\":\"z7X3Po2vlaju3y9QUzmfdSXfGkuJFA0ghD9tItY/1IOfgONREWsx5Dmifd9XQSQyKKpmugfTMe5OSYEjW7Nx2Mw7RbuxNiOK7gBqccrZPg5XIZfKhwfuiHH+SvjlgRBOOyTc0050msre3tEgd/hMALZ3DTplbOFMsoly80qvV24=\"}]},"
            + "{\"@type\":\"InputMask\",\"field\":{\"@type\":\"Gfp\",\"name\":\"gfp\",\"elementSize\":128},\"shares\":[{\"value\":\"NGP5T14EepHf+ydA3EW9hfGuxB/PDwA3Fz9IjlPpSlDitgOaagTKr0kRCs6tPyeE1UeXP0t60RhrAD/iO1Ky7daqlx2R3TLhev6st08rVcfVgloyLyX+nb6xnlw0G8FghWBGpAOjW4dSBfKGXtip+oYw8VAoQaWA471gFeLuBCU=\"}]}]"
            + "}";

    JsonMappingException jme =
        assertThrows(
            JsonMappingException.class,
            () -> objectMapper.readValue(missingMacData, TupleList.class));
    assertThat(jme.getMessage()).startsWith("Missing required creator property 'mac'");
  }

  @SneakyThrows
  @Test
  void givenValidTupleList_whenJsonSerializationRoundTrip_thenRecoverInitialObject() {
    for (TupleType type : TupleType.values()) {
      byte[] tupleData =
          IntStream.range(0, type.getArity() * 2)
              .mapToObj(i -> RandomUtils.nextBytes(type.getField().getElementSize()))
              .reduce(ArrayUtils::addAll)
              .orElseGet(() -> new byte[0]);
      TupleList expectedTupleList =
          TupleList.fromStream(
              type.getTupleCls(),
              type.getField(),
              new ByteArrayInputStream(tupleData),
              tupleData.length);
      String actualJsonString = objectMapper.writeValueAsString(expectedTupleList);
      TupleList actualTupleList = objectMapper.readValue(actualJsonString, TupleList.class);
      assertEquals(expectedTupleList, actualTupleList);
    }
  }

  @SneakyThrows
  @Test
  void givenEmptyTupleList_whenSerializationRoundTrip_thenRecoverExpectedList() {
    for (TupleType type : TupleType.values()) {
      TupleList expectedTupleList =
          TupleList.fromStream(
              type.getTupleCls(), type.getField(), new ByteArrayInputStream(new byte[0]), 0);
      String actualJsonString = objectMapper.writeValueAsString(expectedTupleList);
      TupleList actualTupleList = objectMapper.readValue(actualJsonString, TupleList.class);
      assertEquals(expectedTupleList, actualTupleList);
    }
  }

  @SneakyThrows
  @Test
  void givenValidTupleList_whenExportAsChunk_thenReturnExpectedTupleChunk() {
    UUID expectedUUID = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    byte[] tupleValueData = RandomUtils.nextBytes(Field.GFP.getElementSize());
    byte[] tupleMacData = RandomUtils.nextBytes(Field.GFP.getElementSize());

    Bit<Field.Gfp> expectedTuple1 = new Bit<>(Field.GFP, new Share(tupleValueData, tupleMacData));
    TupleList tupleList =
        new TupleList(Bit.class, Field.GFP, Collections.singletonList(expectedTuple1));
    TupleChunk actualTupleChunk = tupleList.asChunk(expectedUUID);
    assertEquals(expectedUUID, actualTupleChunk.getChunkId());
    assertArrayEquals(
        ArrayUtils.addAll(tupleValueData, tupleMacData), actualTupleChunk.getTuples());
    assertEquals(TupleType.BIT_GFP, actualTupleChunk.getTupleType());
  }

  @Test
  void givenWritingToStreamThrowsException_whenExportAsChunk_thenRethrowIOException() {
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    byte[] tupleValueData = RandomUtils.nextBytes(Field.GFP.getElementSize());
    byte[] tupleMacData = RandomUtils.nextBytes(Field.GFP.getElementSize());
    Bit<Field.Gfp> tuple = new Bit<>(Field.GFP, new Share(tupleValueData, tupleMacData));

    IOException expectedException = new IOException("expected");
    try (MockedConstruction<ByteArrayOutputStream> baosMockedConstruction =
        mockConstruction(
            ByteArrayOutputStream.class,
            (mock, settings) -> doThrow(expectedException).when(mock).write(any(byte[].class)))) {
      TupleList tupleList = new TupleList(Bit.class, Field.GFP, Collections.singletonList(tuple));
      IOException actualException =
          assertThrows(IOException.class, () -> tupleList.asChunk(chunkId));
      assertEquals(expectedException, actualException);
      assertEquals(1, baosMockedConstruction.constructed().size());
    }
  }
}

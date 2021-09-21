/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

public class ShareTest {

  @Test
  public void givenInvalidMacData_whenCreateNew_thenThrowIllegalArgumentException() {
    IllegalArgumentException actualIae =
        assertThrows(IllegalArgumentException.class, () -> new Share(new byte[16], new byte[1]));
    Assert.assertEquals(Share.DATA_MUST_BE_SAME_LENGTH_EXCEPTION_MESSAGE, actualIae.getMessage());
  }

  @Test
  public void givenInvalidValueData_whenCreateNew_thenThrowIllegalArgumentException() {
    IllegalArgumentException actualIae =
        assertThrows(IllegalArgumentException.class, () -> new Share(new byte[1], new byte[16]));
    Assert.assertEquals(Share.DATA_MUST_BE_SAME_LENGTH_EXCEPTION_MESSAGE, actualIae.getMessage());
  }

  @Test
  public void givenValidShareData_whenCreate_thenReturnExpectedShare() {
    byte[] expectedTupleValueData = RandomUtils.nextBytes(Field.GFP.getElementSize());
    byte[] expectedTupleMacData = RandomUtils.nextBytes(Field.GFP.getElementSize());
    Share actualShare = new Share(expectedTupleValueData, expectedTupleMacData);
    assertEquals(expectedTupleMacData, actualShare.getMac());
    assertEquals(expectedTupleValueData, actualShare.getValue());
  }
}

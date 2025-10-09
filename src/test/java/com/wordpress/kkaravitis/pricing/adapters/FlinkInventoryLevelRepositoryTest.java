/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.adapters;

import com.wordpress.kkaravitis.pricing.domain.PricingException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FlinkInventoryLevelRepositoryTest {

    @Mock
    private ValueState<Integer> state;

    private FlinkInventoryLevelRepository repo;

    @BeforeEach
    void setUp() throws Exception {
        repo = new FlinkInventoryLevelRepository();
        // inject the mocked ValueState into the private 'state' field
        Field f = FlinkInventoryLevelRepository.class.getDeclaredField("state");
        f.setAccessible(true);
        f.set(repo, state);
    }

    @Test
    void updateLevel_success() throws Exception {
        // when
        repo.updateLevel(42);

        // then
        verify(state).update(42);
    }

    @Test
    void updateLevel_ioException_throwsPricingException() throws Exception {
        // given
        doThrow(new IOException("disk fail")).when(state).update(99);

        // when
        PricingException ex = assertThrows(
              PricingException.class,
              () -> repo.updateLevel(99)
        );

        // then
        assertTrue(ex.getMessage().contains("Failed to update the inventory level flink state."));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof IOException);
        assertEquals("disk fail", ex.getCause().getMessage());
    }

    @Test
    void getInventoryLevel_valuePresent_returnsThatLevel() throws Exception {
        // given
        when(state.value()).thenReturn(123);

        // when
        int lvl = repo.getInventoryLevel("ignored-productId");

        // then
        assertEquals(123, lvl);
    }

    @Test
    void getInventoryLevel_nullValue_returnsZero() throws Exception {
        // given
        when(state.value()).thenReturn(null);

        // when
        int lvl = repo.getInventoryLevel("ignored");

        // then
        assertEquals(0, lvl);
    }

    @Test
    void getInventoryLevel_ioException_throwsPricingException() throws Exception {
        // given
        when(state.value()).thenThrow(new IOException("fetch error"));

        // when
        PricingException ex = assertThrows(
              PricingException.class,
              () -> repo.getInventoryLevel("pid")
        );

        // then
        assertTrue(ex.getMessage().contains("Failed to fetch inventory level flink state."));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof IOException);
        assertEquals("fetch error", ex.getCause().getMessage());
    }
}
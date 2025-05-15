package com.wordpress.kkaravitis.pricing.adapters;

import com.wordpress.kkaravitis.pricing.domain.PriceRule;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import org.apache.flink.api.common.state.ValueState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FlinkPriceRuleRepositoryTest {

    @Mock
    private ValueState<PriceRule> state;

    private FlinkPriceRuleRepository repo;

    @BeforeEach
    void setUp() throws Exception {
        repo = new FlinkPriceRuleRepository();

        Field f = FlinkPriceRuleRepository.class.getDeclaredField("state");
        f.setAccessible(true);
        f.set(repo, state);
    }

    @Test
    void updateRule_success() throws Exception {
        PriceRule rule = new PriceRule(new Money(5.00, "USD"), new Money(10.00, "USD"));

        // when
        repo.updateRule(rule);

        // then
        verify(state).update(rule);
    }

    @Test
    void updateRule_ioException_throwsPricingException() throws Exception {
        PriceRule rule = new PriceRule(new Money(1.23, "USD"), new Money(4.56, "USD"));
        doThrow(new IOException("disk failure")).when(state).update(rule);

        PricingException ex = assertThrows(
              PricingException.class,
              () -> repo.updateRule(rule)
        );

        assertTrue(ex.getMessage().contains("Failed to update price rule in flink state."));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof IOException);
        assertEquals("disk failure", ex.getCause().getMessage());
    }

    @Test
    void getPriceRule_valuePresent_returnsRule() throws Exception {
        PriceRule rule = new PriceRule(new Money(2.00, "USD"), new Money(3.00, "USD"));
        when(state.value()).thenReturn(rule);

        PriceRule result = repo.getPriceRule("ignored");

        assertSame(rule, result);
    }

    @Test
    void getPriceRule_nullValue_returnsNull() throws Exception {
        when(state.value()).thenReturn(null);

        PriceRule result = repo.getPriceRule("any");

        assertNull(result);
    }

    @Test
    void getPriceRule_ioException_throwsPricingException() throws Exception {
        when(state.value()).thenThrow(new IOException("fetch error"));

        PricingException ex = assertThrows(
              PricingException.class,
              () -> repo.getPriceRule("pid")
        );

        assertTrue(ex.getMessage().contains("Failed to fetch price rule from flink state."));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof IOException);
        assertEquals("fetch error", ex.getCause().getMessage());
    }
}
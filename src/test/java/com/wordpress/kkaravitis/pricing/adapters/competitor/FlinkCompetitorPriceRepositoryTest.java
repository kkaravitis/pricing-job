package com.wordpress.kkaravitis.pricing.adapters.competitor;

import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.Money;
import org.apache.flink.api.common.state.ValueState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import java.lang.reflect.Field;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FlinkCompetitorPriceRepositoryTest {

    @Mock
    private ValueState<CompetitorPrice> state;

    private FlinkCompetitorPriceRepository competitorPriceRepository;

    @BeforeEach
    void setUp() throws Exception {
        competitorPriceRepository = new FlinkCompetitorPriceRepository();
        // inject the mocked ValueState into the private 'state' field
        Field f = FlinkCompetitorPriceRepository.class.getDeclaredField("state");
        f.setAccessible(true);
        f.set(competitorPriceRepository, state);
    }

    @Test
    void getCompetitorPrice_whenStateIsNull_returnsDefaultZeroUsd() throws Exception {
        String pid = "product-123";
        when(state.value()).thenReturn(null);

        CompetitorPrice result = competitorPriceRepository.getCompetitorPrice(pid);

        assertEquals(pid, result.productId());
        assertEquals(new Money(0.0, "USD"), result.price());
    }

    @Test
    void getCompetitorPrice_whenStateNotNull_returnsStateValue() throws Exception {
        CompetitorPrice stored = new CompetitorPrice("p2", new Money(5.25, "USD"));
        when(state.value()).thenReturn(stored);

        CompetitorPrice result = competitorPriceRepository.getCompetitorPrice("p2");

        assertSame(stored, result);
    }

    @Test
    void updatePrice_invokesStateUpdate() throws Exception {
        CompetitorPrice cp = new CompetitorPrice("p3", new Money(3.14, "USD"));
        competitorPriceRepository.updatePrice(cp);
        verify(state).update(cp);
    }
}
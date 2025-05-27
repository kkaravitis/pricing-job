package com.wordpress.kkaravitis.pricing.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.jupiter.api.Test;


public class MyTest {

    @Test
    public void test() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String json = """ 
              {
                "orderId" : "12331123",
                "productId" : "213",
                "quantity" : 1,
                "timestamp" : 123123 
              }
              """;
        OrderEvent event = objectMapper.readValue(json, OrderEvent.class);
        System.out.println(objectMapper.writeValueAsString(event));
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    static class TestOrder {
        private String orderId;
        private String productId;
        private Integer quantity;
        private Long timestamp;
    }



}

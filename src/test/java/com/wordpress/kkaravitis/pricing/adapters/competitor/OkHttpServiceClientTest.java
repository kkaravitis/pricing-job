package com.wordpress.kkaravitis.pricing.adapters.competitor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.wordpress.kkaravitis.pricing.domain.PricingException;
import java.io.IOException;
import java.lang.reflect.Field;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OkHttpServiceClientTest {

    @Mock
    private OkHttpClient okHttpClient;

    @Mock
    private Call mockCall;

    private OkHttpServiceClient serviceClient;

    @BeforeEach
    void setUp() throws Exception {
        serviceClient = new OkHttpServiceClient();
        Field clientField = OkHttpServiceClient.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(serviceClient, okHttpClient);
        // Have newCall() return our mockCall
        when(okHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
    }

    @Test
    void testGet_successfulResponse_returnsBodyString() throws Exception {
        String url = "http://example.com/data";
        String expected = "{\"price\":100}";

        // Build a real HTTP response
        MediaType mediaType = MediaType.get("application/json");
        ResponseBody body = ResponseBody.create(expected, mediaType);
        Response response = new Response.Builder()
              .request(new Request.Builder().url(url).build())
              .protocol(Protocol.HTTP_1_1)
              .code(200)
              .message("OK")
              .body(body)
              .build();

        when(mockCall.execute()).thenReturn(response);

        String result = serviceClient.get(url);
        verify(okHttpClient, times(1)).newCall(any(Request.class));
        assertEquals(expected, result);
    }

    @Test
    void testGet_unsuccessfulResponse_throwsPricingExceptionWithCode() throws Exception {
        String url = "http://example.com/fail";

        // Build a real HTTP 500 response
        Response response = new Response.Builder()
              .request(new Request.Builder().url(url).build())
              .protocol(Protocol.HTTP_1_1)
              .code(500)
              .message("Internal Server Error")
              .body(ResponseBody.create("", null))
              .build();

        when(mockCall.execute()).thenReturn(response);

        PricingException ex = assertThrows(PricingException.class, () -> serviceClient.get(url));
        assertTrue(ex.getMessage().contains("HTTP 500 for " + url));
    }

    @Test
    void testGet_ioExceptionDuringExecute_wrapsInPricingException() throws Exception {
        String url = "http://example.com/error";
        when(mockCall.execute()).thenThrow(new IOException("network down"));

        PricingException ex = assertThrows(PricingException.class, () -> serviceClient.get(url));
        assertTrue(ex.getMessage().contains("Failed to communicate with competitor site."));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof IOException);
    }

    @Test
    void testGet_notFound404_returnsNull() throws Exception {
        String url = "http://example.com/missing";
        // simulate 404 without throwing
        Response response404 = new Response.Builder()
              .request(new Request.Builder().url(url).build())
              .protocol(Protocol.HTTP_1_1)
              .code(404)
              .message("Not Found")
              .body(ResponseBody.create("", null))
              .build();

        when(mockCall.execute()).thenReturn(response404);

        String result = serviceClient.get(url);
        assertNull(result, "404 response should return null, not throw");
    }

}
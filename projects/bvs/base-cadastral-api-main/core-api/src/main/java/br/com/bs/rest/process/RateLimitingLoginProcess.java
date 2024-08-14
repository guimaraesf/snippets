package br.com.bs.rest.process;

import br.com.bs.ratelimiting.service.RateLimitingLoginService;
import org.apache.camel.AsyncCallback;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.util.AsyncProcessorHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class RateLimitingLoginProcess implements AsyncProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitingLoginProcess.class);

    private final RateLimitingLoginService rateLimitingLoginService;

    public RateLimitingLoginProcess(RateLimitingLoginService rateLimitingLoginService) {
        this.rateLimitingLoginService = rateLimitingLoginService;
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        try {
            if (this.rateLimitingLoginService.isEnabled()) {
                Optional.ofNullable(exchange.getProperty("clientIP", String.class))
                        .ifPresent(it ->
                                CompletableFuture.runAsync(() ->
                                        this.rateLimitingLoginService.incrementFailedRequestsByIP(it)
                                )
                        );
            }
        } catch (Exception e) {
            LOGGER.error("Falha ao incrementar Rate Limiting", e);
            exchange.setException(e);
        } finally {
            callback.done(false);
        }
        return true;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        AsyncProcessorHelper.process(this, exchange);
    }

}

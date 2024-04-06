package br.com.bs.rest.process;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.actuate.redis.RedisHealthIndicator;

public class HealthCheckProcess extends AbstractHealthIndicator implements Processor {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final RedisHealthIndicator redisHealthIndicator;

    private static HealthCheckProcess instance;

    public HealthCheckProcess(RedisHealthIndicator redisHealthIndicator) {
        this.redisHealthIndicator = redisHealthIndicator;
    }

    public static HealthCheckProcess getInstance(RedisHealthIndicator redisHealthIndicator) {
        if (instance == null)
            instance = new HealthCheckProcess(redisHealthIndicator);
        return instance;
    }

    @Override
    public void process(Exchange e) throws Exception {
        Health health = health();
        e.getOut().setBody("{ \"status\": \"" + health.getStatus() + "\", \"redis\": \"" + health.getDetails().getOrDefault("redis", Status.UNKNOWN) + "\"}");
        if (!health.getStatus().equals(Status.UP)) {
            e.getOut().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
        }
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        Status redisStatus = this.redisHealthIndicator.health().getStatus();
        builder.status(redisStatus);
        builder.withDetail("redis", redisStatus);
    }
}
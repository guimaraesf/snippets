package br.com.bs.ratelimiting.service.impl;

import br.com.bs.cache.infrastructure.config.CachePrefixEnum;
import br.com.bs.cache.infrastructure.service.CacheService;
import br.com.bs.ratelimiting.service.RateLimitingLoginService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class RateLimitingLoginServiceImpl implements RateLimitingLoginService {

    @Value("${app.rate-limiting.login.enabled}")
    private boolean enabled;

    @Value("${app.rate-limiting.login.attempts}")
    private int loginAttempts;

    @Value("${app.rate-limiting.login.block-seconds}")
    private long loginBlockSeconds;

    private final CacheService cacheService;

    public RateLimitingLoginServiceImpl(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public boolean isEnabled() {
        return this.enabled;
    }

    @Override
    public Boolean isBlockedIP(String ip) {
        if (!this.isEnabled()) {
            return false;
        }
        return Optional.ofNullable(ip)
                .flatMap(it -> this.cacheService.getLong(CachePrefixEnum.RATE_LIMITING, ip))
                .map(it -> it >= this.loginAttempts)
                .orElse(false);
    }

    @Override
    public void incrementFailedRequestsByIP(String ip) {
        if (!this.isEnabled()) {
            return;
        }
        Optional.ofNullable(ip)
                .ifPresent(it ->
                        this.cacheService.increment(CachePrefixEnum.RATE_LIMITING, it, loginBlockSeconds)
                );
    }
}

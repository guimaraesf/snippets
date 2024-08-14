package br.com.bs.serpro.infrastructure.auth.impl;

import br.com.bs.cache.infrastructure.config.CachePrefixEnum;
import br.com.bs.cache.infrastructure.service.CacheService;
import br.com.bs.serpro.infrastructure.auth.SerproAuthService;
import br.com.bs.serpro.infrastructure.auth.data.TokenData;
import br.com.bs.serpro.infrastructure.client.SerproAuthClient;
import br.com.bs.serpro.infrastructure.client.exception.SerproUnauthorizedException;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

@Service
public class SerproAuthServiceImpl implements SerproAuthService {

    private static final CachePrefixEnum CACHE_TOKEN_PREFIX = CachePrefixEnum.SERPRO;
    private static final String CACHE_TOKEN_KEY = "token";

    private final boolean isProduction;

    private final String defaultTokenTrial;

    private final SerproAuthClient serproAuthClient;

    private final CacheService cacheService;

    private final Map<String, ?> requestDataToken = Collections.singletonMap("grant_type", "client_credentials");


    public SerproAuthServiceImpl(Environment environment, SerproAuthClient serproAuthClient, CacheService cacheService) {
        this.isProduction = checkIsProduction(environment);
        this.defaultTokenTrial = environment.getProperty("serpro.auth.token-trial");
        this.serproAuthClient = serproAuthClient;
        this.cacheService = cacheService;
    }

    @Override
    public String getToken() {
        if (!this.isProduction) {
            return this.defaultTokenTrial;
        }
        return this.getTokenFromCache()
                .orElseGet(() -> {
                    TokenData tokenData = serproAuthClient.token(requestDataToken)
                            .getBody();
                    if (tokenData == null) {
                        throw new SerproUnauthorizedException(null);
                    }
                    String token = tokenData.getAccessToken();
                    Integer ttl = tokenData.getExpiresIn() - 3;
                    if (ttl >= 6) {
                        this.cacheService.addCache(CACHE_TOKEN_PREFIX, CACHE_TOKEN_KEY, token, ttl.longValue());
                    }
                    return token;
                });
    }

    private Optional<String> getTokenFromCache() {
        return this.cacheService.getCache(CACHE_TOKEN_PREFIX, CACHE_TOKEN_KEY);
    }

    private static boolean checkIsProduction(Environment environment) {
        return Arrays.asList(environment.getActiveProfiles()).contains("prod");
    }
}

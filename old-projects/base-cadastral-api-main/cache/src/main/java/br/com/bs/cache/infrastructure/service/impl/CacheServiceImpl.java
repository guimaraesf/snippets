package br.com.bs.cache.infrastructure.service.impl;

import br.com.bs.cache.infrastructure.config.CachePrefixEnum;
import br.com.bs.cache.infrastructure.service.CacheService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
public class CacheServiceImpl implements CacheService {

    private final RedisTemplate<?, ?> template;

    @Resource(name = "redisTemplateJson")
    private ValueOperations<String, Object> valueOpsJson;

    @Resource(name = "redisTemplate")
    private ValueOperations<String, Long> valueOpsLong;

    public CacheServiceImpl(
            @Qualifier("redisTemplate") RedisTemplate<?, ?> template
    ) {
        this.template = template;
    }

    @Override
    public <T> void addCache(CachePrefixEnum prefix, String key, T value) {
        Optional.ofNullable(value)
                .ifPresent(it ->
                        this.valueOpsJson
                                .set(key(prefix, key), value)
                );
    }

    @Override
    public <T> void addCache(CachePrefixEnum prefix, String key, T value, Long ttl) {
        Optional.ofNullable(value)
                .ifPresent(it ->
                        this.valueOpsJson
                                .set(key(prefix, key), value, ttl, TimeUnit.SECONDS)
                );
    }

    @Override
    public void addCache(CachePrefixEnum prefix, String key, String value, Long ttl) {
        Optional.ofNullable(value)
                .ifPresent(it ->
                        this.valueOpsJson
                                .set(key(prefix, key), value, ttl, TimeUnit.SECONDS)
                );
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getCache(CachePrefixEnum prefix, String key) {
        return Optional.ofNullable((T) this.valueOpsJson.get(key(prefix, key)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void increment(CachePrefixEnum prefix, String key, Long ttl) {
        this.template.executePipelined(new SessionCallback<Object>() {
            final String newKey = key(prefix, key);

            public Object execute(RedisOperations operations) {
                operations.opsForValue().increment(newKey);
                operations.expire(newKey, ttl, TimeUnit.SECONDS);
                return null;
            }
        });
    }

    @Override
    public Optional<Long> getLong(CachePrefixEnum prefix, String key) {
        return Optional.ofNullable((Object) this.valueOpsLong.get(key(prefix, key)))
                .map(it -> Long.valueOf(it.toString()));
    }

    private static String key(CachePrefixEnum prefix, String key) {
        return prefix.getName() + ":" + key;
    }

}

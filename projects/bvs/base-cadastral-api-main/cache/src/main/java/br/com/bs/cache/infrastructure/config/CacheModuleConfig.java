package br.com.bs.cache.infrastructure.config;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.Arrays;
import java.util.HashSet;

@EnableCaching
@Configuration
public class CacheModuleConfig extends CachingConfigurerSupport {

    @Bean
    public CacheManager cacheManager(LettuceConnectionFactory lettuceConnectionFactory) {
        return RedisCacheManager.builder(lettuceConnectionFactory)
                .initialCacheNames(new HashSet<>(Arrays.asList("rate-limiting", "serpro")))
//                .cacheDefaults(RedisCacheConfiguration.defaultCacheConfig().serializeValuesWith((RedisSerializationContext.SerializationPair<?>) new StringRedisSerializer()) )
                .build();
    }
}

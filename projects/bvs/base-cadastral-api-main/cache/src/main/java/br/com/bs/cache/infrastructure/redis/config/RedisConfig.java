package br.com.bs.cache.infrastructure.redis.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SslOptions;
import io.lettuce.core.TimeoutOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.io.File;
import java.time.Duration;

@Configuration
public class RedisConfig {

    @Value("${app.redis.host}")
    private String host;

    @Value("${app.redis.port}")
    private int port;

    @Value("${app.redis.database}")
    private int database;

    @Value("${app.redis.password}")
    private String password;

    @Value("${app.redis.pem-file}")
    private String pemFile;

    @Value("${app.redis.timeout}")
    private int timeout;

    @Value("${app.redis.timeout-commands}")
    private boolean timeoutCommands;

    @Value("${app.redis.keep-alive}")
    private boolean keepAlive;

    @Bean
    public LettuceConnectionFactory lettuceConnectionFactory() {
        RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration(
                this.host,
                this.port
        );
        redisStandaloneConfiguration.setDatabase(this.database);
        redisStandaloneConfiguration.setPassword(RedisPassword.of(this.password));

        Duration timeoutDuration = Duration.ofSeconds(this.timeout);

        LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder()
                .useSsl()
                .disablePeerVerification()
                .and()
                .clientOptions(
                        ClientOptions.builder()
                                .timeoutOptions(
                                        this.timeoutCommands ? TimeoutOptions.enabled(timeoutDuration) : TimeoutOptions.create()
                                )
                                .socketOptions(
                                        SocketOptions.builder()
                                                .connectTimeout(timeoutDuration)
                                                .keepAlive(this.keepAlive)
                                                .build()
                                )
                                .sslOptions(
                                        SslOptions.builder()
                                                .jdkSslProvider()
                                                .trustManager(new File(this.pemFile))
                                                .build()
                                )
                                .build()
                )
                .build();

        return new LettuceConnectionFactory(redisStandaloneConfiguration, clientConfiguration);
    }

    @Bean("redisTemplate")
    public RedisTemplate<?, ?> redisTemplate(LettuceConnectionFactory lettuceConnectionFactory) {
        RedisTemplate<?, ?> template = new RedisTemplate<>();
        template.setConnectionFactory(lettuceConnectionFactory);
        template.setDefaultSerializer(new StringRedisSerializer());
        return template;
    }

    @Bean("redisTemplateJson")
    public RedisTemplate<String, Object> redisTemplateJson(LettuceConnectionFactory lettuceConnectionFactory) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(JsonGenerator.Feature.IGNORE_UNKNOWN);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.disable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE);
        objectMapper.registerModule(new ParameterNamesModule());
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);

        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(lettuceConnectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer(objectMapper));
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer(objectMapper));
        return template;
    }

}

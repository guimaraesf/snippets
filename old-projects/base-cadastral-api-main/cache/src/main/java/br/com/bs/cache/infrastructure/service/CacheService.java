package br.com.bs.cache.infrastructure.service;

import br.com.bs.cache.infrastructure.config.CachePrefixEnum;

import java.util.Optional;

public interface CacheService {

    <T> void addCache(CachePrefixEnum prefix, String key, T value);

    <T> void addCache(CachePrefixEnum prefix, String key, T value, Long ttl);

    void addCache(CachePrefixEnum prefix, String key, String value, Long ttl);

    <T> Optional<T> getCache(CachePrefixEnum prefix, String key);

    void increment(CachePrefixEnum prefix, String key, Long ttl);

    Optional<Long> getLong(CachePrefixEnum prefix, String key);

}

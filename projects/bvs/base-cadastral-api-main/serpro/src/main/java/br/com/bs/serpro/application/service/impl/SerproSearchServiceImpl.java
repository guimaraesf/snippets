package br.com.bs.serpro.application.service.impl;

import br.com.bs.cache.infrastructure.config.CachePrefixEnum;
import br.com.bs.cache.infrastructure.service.CacheService;
import br.com.bs.serpro.application.service.SerproSearchService;
import br.com.bs.serpro.application.service.data.SerproSearchData;
import br.com.bs.serpro.infrastructure.client.SerproClient;
import br.com.bs.serpro.infrastructure.client.data.legalperson.LegalPersonData;
import br.com.bs.serpro.infrastructure.client.data.physicalperson.PhysicalPersonData;
import br.com.bs.serpro.infrastructure.client.exception.SerproDocumentNotFoundException;
import br.com.bs.serpro.infrastructure.client.exception.SerproUnderageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class SerproSearchServiceImpl implements SerproSearchService {

    private final SerproClient serproClient;

    private final CacheService cacheService;

    private final boolean isEnabledSearch;

    private final boolean isEnabledPhysicalPersonSearch;

    private final boolean isEnabledPhysicalPersonCacheSearch;

    private final boolean isEnabledLegalPersonSearch;

    private final boolean isEnabledLegalPersonCacheSearch;

    private final Long cacheTTLNotFound;

    private final Long cacheTTLUnderage;

    private final Logger LOGGER = LoggerFactory.getLogger(SerproSearchServiceImpl.class);

    public SerproSearchServiceImpl(
            SerproClient serproClient,
            CacheService cacheService,
            @Value("${serpro.enabled}") Boolean enabledSearch,
            @Value("${serpro.cpf.enabled}") Boolean enabledCPF,
            @Value("${serpro.cnpj.enabled}") Boolean enabledCNPJ,
            @Value("${serpro.cache.cpf}") Boolean enabledCPFCache,
            @Value("${serpro.cache.cnpj}") Boolean enabledCNPJCache,
            @Value("${serpro.cache.ttl.not-found}") Long cacheTTLNotFound,
            @Value("${serpro.cache.ttl.underage}") Long cacheTTLUnderage
    ) {
        this.serproClient = serproClient;
        this.cacheService = cacheService;
        this.isEnabledSearch = enabledSearch;
        this.isEnabledPhysicalPersonSearch = enabledCPF;
        this.isEnabledPhysicalPersonCacheSearch = enabledCPFCache;
        this.isEnabledLegalPersonSearch = enabledCNPJ;
        this.isEnabledLegalPersonCacheSearch = enabledCNPJCache;
        this.cacheTTLNotFound = cacheTTLNotFound;
        this.cacheTTLUnderage = cacheTTLUnderage;
    }

    @Override
    public Optional<PhysicalPersonData> searchPhysicalPerson(String cpf, boolean onlyFromCache) {
        try {
            if (!this.isEnabledSearch || !this.isEnabledPhysicalPersonCacheSearch) {
                return Optional.empty();
            }
            Optional<SerproSearchData> cache = this.getPhysicalPersonFromCache(cpf);
            cache.ifPresent(it -> {
                if (it.getIsNotFound()) {
                    throw new SerproDocumentNotFoundException(null);
                }
                if (it.getIsUnderage()) {
                    throw new SerproUnderageException(null);
                }
            });
            if (cache.isPresent()) {
                return cache.map(it -> it.getPhysicalPerson());
            }
            if (!this.isEnabledPhysicalPersonSearch || onlyFromCache) {
                return Optional.empty();
            }
            Optional<PhysicalPersonData> physicalPerson = Optional.ofNullable(this.serproClient.physicalPerson(cpf).getBody());
            physicalPerson.ifPresent(it -> this.cacheService.addCache(CachePrefixEnum.SERPRO_CPF, cpf, SerproSearchData.create(it)));
            return physicalPerson;
        } catch (SerproDocumentNotFoundException e) {
            this.cacheService.addCache(CachePrefixEnum.SERPRO_CPF, cpf, SerproSearchData.createNotFound(cpf), this.cacheTTLNotFound);
            throw e;
        } catch (SerproUnderageException e) {
            this.cacheService.addCache(CachePrefixEnum.SERPRO_CPF, cpf, SerproSearchData.createUnderage(cpf), this.cacheTTLUnderage);
            throw e;
        } catch (Exception e) {
            LOGGER.error("Falha ao consultar CPF na SERPRO", e);
            throw e;
        }
    }

    @Override
    public Optional<LegalPersonData> searchLegalPerson(String cnpj) {
        try {
            if (!this.isEnabledSearch || !this.isEnabledLegalPersonCacheSearch) {
                return Optional.empty();
            }
            Optional<SerproSearchData> cache = this.getLegalPersonFromCache(cnpj);
            cache.ifPresent(it -> {
                if (it.getIsNotFound()) {
                    throw new SerproDocumentNotFoundException(null);
                }
            });
            if (cache.isPresent()) {
                return cache.map(it -> it.getLegalPerson());
            }
            if (!this.isEnabledLegalPersonSearch) {
                return Optional.empty();
            }
            Optional<LegalPersonData> legalPerson = Optional.ofNullable(this.serproClient.legalPerson(cnpj).getBody());
            legalPerson.ifPresent(it -> this.cacheService.addCache(CachePrefixEnum.SERPRO_CNPJ, cnpj, SerproSearchData.create(it)));
            return legalPerson;
        } catch (SerproDocumentNotFoundException e) {
            this.cacheService.addCache(CachePrefixEnum.SERPRO_CNPJ, cnpj, SerproSearchData.createNotFound(cnpj), this.cacheTTLNotFound);
            throw e;
        } catch (Exception e) {
            LOGGER.error("Falha ao consultar CNPJ na SERPRO", e);
            throw e;
        }
    }

    private Optional<SerproSearchData> getPhysicalPersonFromCache(String cpf) {
        return this.cacheService.getCache(CachePrefixEnum.SERPRO_CPF, cpf);
    }

    private Optional<SerproSearchData> getLegalPersonFromCache(String cnpj) {
        return this.cacheService.getCache(CachePrefixEnum.SERPRO_CNPJ, cnpj);
    }

}

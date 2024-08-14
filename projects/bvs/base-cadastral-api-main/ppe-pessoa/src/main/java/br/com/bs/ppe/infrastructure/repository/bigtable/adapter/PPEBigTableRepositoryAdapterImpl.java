package br.com.bs.ppe.infrastructure.repository.bigtable.adapter;

import br.com.bs.ppe.domain.data.PPETypeEnum;
import br.com.bs.ppe.domain.entity.MandateEntity;
import br.com.bs.ppe.domain.entity.OrganizationEntity;
import br.com.bs.ppe.domain.entity.PPEEntity;
import br.com.bs.ppe.domain.entity.RelationshipEntity;
import br.com.bs.ppe.domain.entity.vo.OrganizationAddressVO;
import br.com.bs.ppe.domain.repository.PPERepository;
import br.com.bs.ppe.infrastructure.repository.bigtable.PPEBigTableRepository;
import br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories.MandateFactory;
import br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories.OrganizationFactory;
import br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories.PPEFactory;
import br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories.RelationshipFactory;
import br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories.vo.OrganizationAddressVOFactory;
import br.com.bs.ppe.infrastructure.repository.bigtable.connection.BigTableConnection;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Repository
@Profile("bigtable")
public class PPEBigTableRepositoryAdapterImpl implements PPERepository {

    private final Logger LOGGER = LoggerFactory.getLogger(PPEBigTableRepositoryAdapterImpl.class);

    private static final int LIMIT_MANDATES = 10;

    private final PPEBigTableRepository ppeBigTableRepository;

    private final BigTableConnection bigTableConnection;

    public PPEBigTableRepositoryAdapterImpl(PPEBigTableRepository ppeBigTableRepository, BigTableConnection bigTableConnection) {
        this.ppeBigTableRepository = ppeBigTableRepository;
        this.bigTableConnection = bigTableConnection;
    }

    @Override
    public Optional<PPEEntity> findByDocument(String document) {
        try (BigtableDataClient dataClient = this.bigTableConnection.createDataClient()) {
            return this.ppeBigTableRepository.findPPEPessoas(dataClient, document)
                    .map(it -> {
                        List<RelationshipEntity> relationships = null;
                        List<MandateEntity> mandates = null;
                        if (PPETypeEnum.TITLEHOLDER.getCodeType().equals(it.getTypeCode())) {
                            mandates = this.ppeBigTableRepository.findMandates(dataClient, document)
                                    .stream()
                                    .map(mandate -> MandateFactory.create(mandate))
                                    .filter(mandate -> mandate.isValidToShow())
                                    .sorted((o1, o2) -> Boolean.compare(o2.isActive(), o1.isActive()))
                                    .limit(LIMIT_MANDATES)
                                    .collect(Collectors.toList());

                            if (mandates.isEmpty()) {
                                return null;
                            }

                            relationships = this.ppeBigTableRepository.findRelationship(dataClient, document, true)
                                    .stream()
                                    .map(relationship -> RelationshipFactory.create(relationship))
                                    .collect(Collectors.toList());

                            final List<MandateEntity> finalMandates = mandates;
                            CompletableFuture<List<OrganizationEntity>> supplyOrganizations = CompletableFuture.supplyAsync(() ->
                                    finalMandates.stream()
                                            .map(mandate -> mandate.getOrganization().getCode())
                                            .distinct()
                                            .map(organizationCode -> CompletableFuture.supplyAsync(() -> this.ppeBigTableRepository.findOrganization(dataClient, organizationCode)))
                                            .map(async -> async.join())
                                            .filter(organization -> organization.isPresent())
                                            .map(organization -> OrganizationFactory.create(organization.get()))
                                            .collect(Collectors.toList())
                            );

                            CompletableFuture<List<OrganizationAddressVO>> supplyOrganizationAddresses = CompletableFuture.supplyAsync(() ->
                                    finalMandates.stream()
                                            .map(mandate -> mandate.getPublicOfficeCode() + "#" + mandate.getOrganization().getCode())
                                            .distinct()
                                            .map(code -> CompletableFuture.supplyAsync(
                                                    () -> this.ppeBigTableRepository.findOrganizationAddress(dataClient, code)
                                            ))
                                            .map(async -> async.join())
                                            .filter(organizationAddress -> organizationAddress.isPresent())
                                            .map(organizationAddress -> OrganizationAddressVOFactory.create(organizationAddress.get()))
                                            .collect(Collectors.toList())
                            );

                            mandates = supplyOrganizations.thenCombine(supplyOrganizationAddresses, (organizations, organizationAddresses) ->
                                finalMandates.stream()
                                        .map(mandate -> mandate.withOrganization(
                                                organizations
                                                        .stream()
                                                        .filter(organization -> organization.getCode().equals(mandate.getOrganization().getCode()))
                                                        .map(organization ->
                                                                organizationAddresses
                                                                        .stream()
                                                                        .filter(organizationAddress ->
                                                                                organizationAddress.getOrganizationCode().equals(organization.getCode()) &&
                                                                                        organizationAddress.getMandateCode().equals(mandate.getPublicOfficeCode())
                                                                        )
                                                                        .findFirst()
                                                                        .map(organizationAddress -> organization.withAddress(organizationAddress))
                                                                        .orElse(organization)
                                                        )
                                                        .findFirst()
                                                        .orElse(null)
                                        ))
                                        .collect(Collectors.toList())
                            ).join();
                        } else {
                            relationships = this.ppeBigTableRepository.findRelationship(dataClient, document, false)
                                    .stream()
                                    .map(relationship -> RelationshipFactory.create(relationship))
                                    .collect(Collectors.toList());

                            List<String> activeTitleholders = relationships.stream()
                                    .map(relationship -> relationship.getDocumentTitleholder())
                                    .distinct()
                                    .map(cpf -> CompletableFuture.supplyAsync(() -> this.ppeBigTableRepository.findMandates(dataClient, cpf)))
                                    .map(async -> async.join())
                                    .flatMap(titleHolderMandates -> titleHolderMandates.stream())
                                    .map(mandate -> MandateFactory.create(mandate))
                                    .filter(mandate -> mandate.isValidToShow())
                                    .map(titleholderMandates -> titleholderMandates.getDocument())
                                    .distinct()
                                    .collect(Collectors.toList());

                            relationships = relationships.stream()
                                    .filter(relationship -> activeTitleholders.contains(relationship.getDocumentTitleholder()))
                                    .collect(Collectors.toList());

                            if (relationships.isEmpty()) {
                                return null;
                            }

                        }
                        return PPEFactory.create(it, mandates, relationships);
                    });
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable", e);
            return Optional.empty();
        }
    }

}

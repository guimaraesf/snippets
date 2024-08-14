package br.com.bs.ppe.infrastructure.repository.bigtable;

import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.*;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;

import java.util.List;
import java.util.Optional;

public interface PPEBigTableRepository {

    Optional<PPEPessoasEntityModel> findPPEPessoas(BigtableDataClient dataClient, String document);

    Optional<OrganizationEntityModel> findOrganization(BigtableDataClient dataClient, String code);

    Optional<OrganizationAddressEntityModel> findOrganizationAddress(BigtableDataClient dataClient, String organizationCode, String mandateCode);

    Optional<OrganizationAddressEntityModel> findOrganizationAddress(BigtableDataClient dataClient, String rowKey);

    List<MandateEntityModel> findMandates(BigtableDataClient dataClient, String document);

    List<RelationshipEntityModel> findRelationship(BigtableDataClient dataClient, String document, boolean fromTitleholder);
}

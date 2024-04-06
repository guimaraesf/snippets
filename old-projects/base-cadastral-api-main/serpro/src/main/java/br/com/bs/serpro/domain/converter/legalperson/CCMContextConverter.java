package br.com.bs.serpro.domain.converter.legalperson;

import br.com.bs.serpro.domain.converter.ContextConverter;
import br.com.bs.serpro.infrastructure.client.data.legalperson.LegalPersonData;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class CCMContextConverter implements ContextConverter<LegalPersonData> {

    public static final String CONTEXT = "ccm";

    @Override
    public Map<String, Object> toMap(LegalPersonData data) {
        HashMap<String, Object> row = new HashMap<>();
        row.put("CNPJ", data.getNi());
        row.put("STATUS", null);
        row.put("INSCRICAO_MUNICIPAL", null);
        row.put("EMPRESA", data.getCompanyName());
        Optional.ofNullable(data.getLegalNature())
                .ifPresent(it -> row.put("TIPO_PESSOA_JURIDICA", it.getDescription()));
        row.put("TIPO_UNIDADE", null);
        Optional.ofNullable(data.getAddress())
                .ifPresent(it -> {
                    row.put("ENDERECO_LOCALIZACAO", it.getTypeAddress() + " " + it.getAddress() + " " + it.getNumber());
                    row.put("BAIRRO", it.getNeighborhood());
                    row.put("CEP", it.getZipCode());
                    row.put("TIPO_ENDERECO", it.getTypeAddress());
                });
        Optional.ofNullable(data.getPhones())
                .filter(it -> !it.isEmpty())
                .ifPresent(it -> row.put("TELEFONE", it.get(0).getDdd() + it.get(0).getNumber()));
        row.put("INICIO_DATA", null);
        row.put("DATA_INSCRICAO", null);
        row.put("INSCRICAO_CENTRALIZADORA", null);
        row.put("NUMERO_IPTU", null);
        row.put("DATA_ATUALIZACAO", null);
        row.put("ATIVO", null);
        row.put("DATA_CONSULTA", null);
        return row;
    }

    @Override
    public boolean isApplicable(String context) {
        return CONTEXT.equals(context);
    }
}

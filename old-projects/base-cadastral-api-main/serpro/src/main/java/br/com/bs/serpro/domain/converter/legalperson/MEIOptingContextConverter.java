package br.com.bs.serpro.domain.converter.legalperson;

import br.com.bs.serpro.domain.converter.ContextConverter;
import br.com.bs.serpro.infrastructure.client.data.legalperson.LegalPersonData;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class MEIOptingContextConverter implements ContextConverter<LegalPersonData> {

    public static final String CONTEXT = "opcao_mei";

    @Override
    public Map<String, Object> toMap(LegalPersonData data) {
        HashMap<String, Object> row = new HashMap<>();
        row.put("CNPJ", data.getNi());
        row.put("RET_CONSULTA", null);
        row.put("PORTE", data.getSize());
        Optional.ofNullable(data.getAdditionalInformation())
                .ifPresent(info -> {
                    row.put("OPCAO_PELO_SIMPLES", info.getSimpleOpting());
                    row.put("OPCAO_MEI", info.getMeiOpting());
                    Optional.ofNullable(info.getSimplePeriods())
                            .filter(it -> !it.isEmpty())
                            .ifPresent(it -> {
                                row.put("DATA_OPCAO_PELO_SIMPLES", it.get(it.size() - 1).getInitData());
                                row.put("DATA_EXCLUSAO_SIMPLES", it.get(it.size() - 1).getEndDate());
                            });
                });
        row.put("DATA_HORA_CONSULTA", null);
        return row;
    }

    @Override
    public boolean isApplicable(String context) {
        return CONTEXT.equals(context);
    }
}

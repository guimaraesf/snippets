package br.com.bs.serpro.domain.converter.physicalperson;

import br.com.bs.serpro.domain.converter.ContextConverter;
import br.com.bs.serpro.infrastructure.client.data.physicalperson.PhysicalPersonData;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class ProtocolContextConverter implements ContextConverter<PhysicalPersonData> {

    public static final String CONTEXT = "protocolo";
    public static final String FIELD_CODE_SITUACAO_RECEITA_FEDERAL = "DSC_STT_DCTO_RCTA";

    @Override
    public Map<String, Object> toMap(PhysicalPersonData data) {
        if (data.getSituation() == null) {
            return null;
        }
        HashMap<String, Object> row = new HashMap<>();
        row.put(NameContextConverter.FIELD_DOCUMENT, "000" + data.getNi());
        row.put(FIELD_CODE_SITUACAO_RECEITA_FEDERAL, data.getSituation().getDescription());
        row.put(NameContextConverter.FIELD_CONDITION, "Ativo");
        return row;
    }

    @Override
    public boolean isApplicable(String context) {
        return CONTEXT.equals(context);
    }
}

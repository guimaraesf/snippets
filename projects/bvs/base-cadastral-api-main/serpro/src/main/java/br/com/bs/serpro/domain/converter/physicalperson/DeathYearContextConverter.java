package br.com.bs.serpro.domain.converter.physicalperson;

import br.com.bs.serpro.domain.converter.ContextConverter;
import br.com.bs.serpro.infrastructure.client.data.physicalperson.PhysicalPersonData;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class DeathYearContextConverter implements ContextConverter<PhysicalPersonData> {

    public static final String CONTEXT = "ano_obito";
    public static final String FIELD_DEATH_YEAR = "ANO_OBTO";

    @Override
    public Map<String, Object> toMap(PhysicalPersonData data) {
        if (data.getDeathYear() == null) {
            return null;
        }
        HashMap<String, Object> row = new HashMap<>();
        row.put(NameContextConverter.FIELD_DOCUMENT, "000" + data.getNi());
        row.put(FIELD_DEATH_YEAR, data.getDeathYear());
        row.put(NameContextConverter.FIELD_CONDITION, "Ativo");
        return row;
    }

    @Override
    public boolean isApplicable(String context) {
        return CONTEXT.equals(context);
    }
}

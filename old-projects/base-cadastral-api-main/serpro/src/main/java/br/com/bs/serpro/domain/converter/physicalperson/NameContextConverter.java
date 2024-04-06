package br.com.bs.serpro.domain.converter.physicalperson;

import br.com.bs.serpro.domain.converter.ContextConverter;
import br.com.bs.serpro.infrastructure.client.data.physicalperson.PhysicalPersonData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class NameContextConverter implements ContextConverter<PhysicalPersonData> {

    public static final String CONTEXT = "nome";
    public static final String FIELD_DOCUMENT = "NUM_CPF_CNPJ";
    public static final String FIELD_NAME = "NOM_RAZ_SOC";
    public static final String FIELD_BIRTHDAY = "DAT_NSC";
    public static final String FIELD_CONDITION = "DSC_COND";

    private static final Logger LOGGER = LoggerFactory.getLogger(NameContextConverter.class);

    @Override
    public Map<String, Object> toMap(PhysicalPersonData data) {
        HashMap<String, Object> row = new HashMap<>();
        row.put(FIELD_DOCUMENT, "000" + data.getNi());
        row.put(FIELD_NAME, data.getName().toUpperCase());
        row.put(FIELD_BIRTHDAY, getFormattedBirthday(data));
        row.put(FIELD_CONDITION, "Ativo");
        return row;
    }

    private static String getFormattedBirthday(PhysicalPersonData physicalPerson) {
        return Optional.ofNullable(physicalPerson.getBirthday())
                .filter(it -> !it.isEmpty())
                .map(it -> {
                    try {
                        return LocalDate.parse(
                                it,
                                DateTimeFormatter.ofPattern("ddMMyyyy")
                        ).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    } catch (Exception e) {
                        LOGGER.warn(
                                "SERPRO: Failed to cast birthday of document {}",
                                physicalPerson.getNi(),
                                e
                        );
                        return null;
                    }
                })
                .orElse(null);
    }

    @Override
    public boolean isApplicable(String context) {
        return CONTEXT.equals(context);
    }
}

package br.com.bs.serpro.domain.converter.legalperson;

import br.com.bs.serpro.domain.converter.ContextConverter;
import br.com.bs.serpro.infrastructure.client.data.legalperson.LegalPersonData;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class PJRecipeContextConverter implements ContextConverter<LegalPersonData> {

    public static final String CONTEXT = "receita_pj";

    @Override
    public Map<String, Object> toMap(LegalPersonData data) {
        HashMap<String, Object> row = new HashMap<>();
        row.put("CNPJ", data.getNi());
        row.put("RET_CONSULTA", null);
        row.put("DATA_ABERTURA", data.getOpeningDate());
        row.put("RAZAO_SOC", data.getCompanyName());
        row.put("NOM_FNTS", data.getTradeName());
        Optional.ofNullable(data.getLegalNature())
                .ifPresent(it -> row.put("CODIGO_E_DESCRICAO_DA_NATUREZA_JURIDICA", it.getCode() + " - " + it.getDescription()));
        Optional.ofNullable(data.getPrimaryCnae())
                .ifPresent(it -> row.put("CNAE_1", it.getCode()));
        Optional.ofNullable(data.getAddress())
                .ifPresent(it -> {
                    row.put("TP_LOGR", it.getTypeAddress());
                    row.put("LOGRADOURO", it.getAddress());
                    row.put("NUMERO", it.getNumber());
                    row.put("COMPLEMENTO", it.getComplement());
                    row.put("CEP", it.getZipCode());
                    row.put("BAIRRO", it.getNeighborhood());
                    Optional.ofNullable(it.getCity())
                            .ifPresent(city -> {
                                row.put("COD_MNCP", city.getCode());
                                row.put("MUNICIPIO", city.getDescription());
                            });
                    row.put("UF", it.getState());
                });

        row.put("EMAIL", data.getEmail());
        if (data.getPhones() != null && !data.getPhones().isEmpty()) {
            row.put("TELEFONE", data.getPhones().get(0).getDdd() + data.getPhones().get(0).getNumber());
        }
        row.put("ENTE_FED_RESP", null);
        Optional.ofNullable(data.getSituation())
                .ifPresent(it -> {
                    row.put("SIT_CAD", getSituationNameByCode(it.getCode()));
                    row.put("DT_SIT_CAD", it.getDate());
                    row.put("MOT_SIT_CAD", it.getReason());
                });
        row.put("SIT_ESP", data.getEspecialSituation());
        row.put("DT_SIT_ESP", data.getEspecialSituationDate());
        row.put("MATRIZ", getEstablishmentTypeByCode(data.getEstablishmentType()));
        row.put("DTH_CNSLT", null);
        row.put("RSVD", null);
        row.put("PORTE", getSizeByCode(data.getSize()));
        Optional.ofNullable(data.getSecondariesCnae())
                .ifPresent(it -> {
                    for (int i = 0; i < it.size(); i++) {
                        row.put("CNAE_" + (i + 2), it.get(i).getCode());
                    }
                });
        return row;
    }

    @Override
    public boolean isApplicable(String context) {
        return CONTEXT.equals(context);
    }

    private static String getEstablishmentTypeByCode(String code) {
        return Optional.ofNullable(code)
                .map(it -> {
                    if (it.equals("1")) {
                        return "MATRIZ";
                    }
                    if (it.equals("2")) {
                        return "FILIAL";
                    }
                    return null;
                })
                .orElse(null);
    }

    private static String getSituationNameByCode(String code) {
        return Optional.ofNullable(code)
                .map(it -> {
                    if (it.equals("1")) {
                        return "NULA";
                    }
                    if (it.equals("2")) {
                        return "ATIVA";
                    }
                    if (it.equals("3")) {
                        return "SUPENSA";
                    }
                    if (it.equals("4")) {
                        return "INAPTA";
                    }
                    if (it.equals("5")) {
                        return "ATIVA NAO REGULAR";
                    }
                    if (it.equals("8")) {
                        return "BAIXADA";
                    }
                    return null;
                }).orElse(null);

    }

    private static String getSizeByCode(String code) {
        return Optional.ofNullable(code)
                .map(it -> {
                    if (it.equals("01")) {
                        return "ME";
                    }
                    if (it.equals("03")) {
                        return "EP";
                    }
                    if (it.equals("05")) {
                        return "DEMAIS EMPRESAS";
                    }
                    return null;
                })
                .orElse(null);
    }
}

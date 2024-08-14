package br.com.bs.ppe.domain.entity.vo;

import java.util.HashMap;
import java.util.Map;

public class EsferaPoderVO {

    private static final String FIELD_CODE = "codigo";
    private static final String FIELD_NAME = "nome";

    private final String code;

    private final String name;

    public EsferaPoderVO(String code, String name) {
        this.code = code;
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(FIELD_CODE, this.getCode());
        map.put(FIELD_NAME, this.getName());
        return map;
    }
}

package br.com.bs.ppe.domain.data;

public enum PPETypeEnum {

    TITLEHOLDER("1"),
    RELATIONSHIP("3");

    private final String codeType;

    PPETypeEnum(String codeType) {
        this.codeType = codeType;
    }

    public String getCodeType() {
        return codeType;
    }

}

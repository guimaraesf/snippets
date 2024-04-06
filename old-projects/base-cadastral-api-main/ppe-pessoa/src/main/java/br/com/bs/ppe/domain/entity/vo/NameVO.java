package br.com.bs.ppe.domain.entity.vo;

import br.com.bs.util.CadastralUtil;
import br.com.bs.util.Util;

public class NameVO {

    private final String document;

    private final String birthday;

    private final Integer underageType;

    public NameVO(String document, String birthday) {
        this.document = document;
        this.birthday = birthday;
        this.underageType = Util.isMenorIdade(this.birthday);
    }

    public String getDocument() {
        return document;
    }

    public String getBirthday() {
        return birthday;
    }

    public boolean isUnderage() {
        return this.underageType != 2;
    }

    public boolean isUnder16YearsOld() {
        return this.underageType == 0;
    }

    public String getUnderageMessage() {
        if (!this.isUnderage()) {
            return null;
        }
        return this.isUnder16YearsOld() ? CadastralUtil.MENSAGEM_MENOR_IDADE : CadastralUtil.MENSAGEM_MENOR_IDADE_ACIMA_16_ANOS;
    }
}

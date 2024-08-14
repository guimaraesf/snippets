package br.com.bs.serpro.infrastructure.client;

import br.com.bs.serpro.infrastructure.client.config.SerproConfigClient;
import br.com.bs.serpro.infrastructure.client.data.legalperson.LegalPersonData;
import br.com.bs.serpro.infrastructure.client.data.physicalperson.PhysicalPersonData;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(
        value = "serpro",
        url = "${serpro.url}",
        configuration = SerproConfigClient.class
)
public interface SerproClient {

    @GetMapping("${serpro.cpf.path-prefix}/cpf/{cpf}")
    ResponseEntity<PhysicalPersonData> physicalPerson(@PathVariable String cpf);

    @GetMapping("${serpro.cnpj.path-prefix}/{cnpj}")
    ResponseEntity<LegalPersonData> legalPerson(@PathVariable String cnpj);

}

package br.com.bs.serpro.infrastructure.config;

import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableFeignClients(basePackages = "br.com.bs.serpro.infrastructure.client")
public class SerproModuleConfig {
}

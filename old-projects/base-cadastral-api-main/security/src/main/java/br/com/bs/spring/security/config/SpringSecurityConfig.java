package br.com.bs.spring.security.config;

import br.com.bs.util.LoadEncryptedFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableOAuth2Client;

@Configuration
@EnableWebSecurity
@EnableOAuth2Client
public class SpringSecurityConfig extends WebSecurityConfigurerAdapter {

    public Logger LOGGER = LoggerFactory.getLogger(getClass());

    @Autowired
    private Environment env;

    @Autowired
    public void configureGlobal(AuthenticationManagerBuilder auth) throws Exception {

        String[] users;

        String publicKeyFilePath = env.getProperty("app.authentication.public-key-path");
        String encryptFilePath = env.getProperty("app.authentication.encrypt-file");

        LoadEncryptedFiles encryptedFile = new LoadEncryptedFiles(publicKeyFilePath, encryptFilePath);
        users = encryptedFile.decryptFile().split("\n");

        for (String usr : users) {
            String[] userSplit = usr.split(";");
            auth.inMemoryAuthentication().withUser(userSplit[0].trim()).password("{noop}" + userSplit[1].trim()).roles("USER");
        }
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.requestMatchers()
                .antMatchers("/login", "/oauth/authorize", "/api/oauth/dialog")
                .and()
                .authorizeRequests()
                .anyRequest().authenticated()
                .and()
                .formLogin().permitAll();
    }
}
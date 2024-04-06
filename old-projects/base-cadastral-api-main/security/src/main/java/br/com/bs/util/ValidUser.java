package br.com.bs.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ValidUser {

    public Logger LOGGER = LoggerFactory.getLogger(getClass());

    @Autowired
    private Environment env;

    public boolean validUser(String user, String encodedPassword, Boolean isBlockedIP) {
        if (isBlockedIP) {
            return false;
        }
        String publicKeyFilePath = env.getProperty("app.authentication.public-key-path");
        String encryptFilePath = env.getProperty("app.authentication.encrypt-file");

        LoadEncryptedFiles encryptedFile = new LoadEncryptedFiles(publicKeyFilePath, encryptFilePath);
        String[] users;
        Map<String, String> mapUsers;

        //TODO Melhoria: Ajustar bloco para ser executado apenas uma vez, na criação da classe
        users = encryptedFile.decryptFile().split("\n");
        mapUsers = new HashMap();

        for (String usr : users) {
            String[] userSplit = usr.split(";");
            mapUsers.put(userSplit[0], userSplit[1]);
        }
        //=====================================================================================

        users = encryptedFile.decryptFile().split("\n");
        mapUsers = new HashMap();

        for (String usr : users) {
            String[] userSplit = usr.split(";");
            mapUsers.put(userSplit[0], userSplit[1]);
        }

        if (user == null || encodedPassword == null) {
            LOGGER.error("Unauthorized: Username or password is null.");
            return false;
        } else {
            //String encodedSecret = Base64.getEncoder().encodeToString(encodedPassword.getBytes("UTF-8"));

            String password = null;
            try {
                password = new String(Base64.getDecoder().decode(encodedPassword), "UTF-8");
            } catch (Exception e) {
                LOGGER.error("-- ERROR: {}", e);
                return false;
            }

            if (mapUsers.containsKey(user) && mapUsers.get(user).trim().equals(password)) {
                return true;
            } else {
                LOGGER.error("Unauthorized: Invalid username or password.");
                return false;
            }
        }
    }
}


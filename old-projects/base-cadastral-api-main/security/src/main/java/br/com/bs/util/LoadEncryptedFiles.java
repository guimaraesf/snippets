package br.com.bs.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public class LoadEncryptedFiles {

    private String publicKeyFilePath;
    private String encryptFilePath;

    public Logger LOGGER = LoggerFactory.getLogger(getClass());

    public LoadEncryptedFiles(String publicKeyFilePath, String encryptFilePath) {
        this.publicKeyFilePath = publicKeyFilePath;
        this.encryptFilePath = encryptFilePath;
    }

    public PublicKey publicKey() {

        Path publicKeyPath = Paths.get(publicKeyFilePath);
        byte[] publicKeyBytes;
        PublicKey publicKey = null;

        try {
            publicKeyBytes = Files.readAllBytes(publicKeyPath);
            publicKey = KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(publicKeyBytes));
        } catch (Exception e) {
            LOGGER.error("-- ERROR: {}", e);
        }
        return publicKey;
    }

    public String decryptFile() {

        String strFileEncrypted = null;

        File fileEncrypted = new File(encryptFilePath);
        FileInputStream fileIS;
        try {
            fileIS = new FileInputStream(fileEncrypted);
            byte[] data = new byte[(int) fileEncrypted.length()];
            fileIS.read(data);
            fileIS.close();

            strFileEncrypted = new String(data, "UTF-8");
        } catch (Exception e) {
            LOGGER.error("-- ERROR: {}", e);
        }

        Cipher cipher1;
        byte[] encryptedFile;
        byte[] decryptedFile;

        try {
            encryptedFile = Base64.getDecoder().decode(strFileEncrypted);
            cipher1 = Cipher.getInstance("RSA");
            cipher1.init(Cipher.DECRYPT_MODE, publicKey());
            decryptedFile = cipher1.doFinal(encryptedFile);

            return new String(decryptedFile);

        } catch (Exception e) {
            LOGGER.error("-- ERROR: {}", e);
            //TODO - Ajustar este return
            return "";
        }
    }
}

package br.com.bs.jwt.util;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.jsonwebtoken.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class JwtUtil {

    public static Logger LOGGER = LoggerFactory.getLogger(JwtUtil.class);
    public static int expiration;
    public static String secret;

    @Value("${app.jwt.expiration}")
    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }

    @Value("${app.jwt.secret}")
    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String generateToken(String userName, String product) {

        long curTime = new Date().getTime();
        long expTime = curTime + (this.expiration * 1000);

        byte[] encodedSecret = Base64.getEncoder().encode(secret.getBytes());

        // Header
        Map<String, Object> header = new HashMap<>();
        header.put(Header.TYPE, Header.JWT_TYPE);

        // Payload
        JwtBuilder builder = Jwts.builder()
                .setHeader(header)
                // TODO Melhoria: Adicionar ID dinamicamente
                .setId("JWT-ID")                         // Token ID
                .setIssuer(product)                     // Emissor do Token
                .setAudience(userName)                  // Destinatário do Token
                .setSubject("Access to API Consumo")    // Assunto do Token
                .setExpiration(new Date(expTime))       // Tempo de expiração do Token
                .setIssuedAt(new Date(curTime))         // Data/hora de criação do Token JWT
                .signWith(SignatureAlgorithm.HS256, encodedSecret);     // Algoritmo de assinatura e chave codificada em base64

        String token = builder.compact();

        return token;
    }

    public static boolean validToken(String tokenBearer) throws UnsupportedEncodingException {

        if(tokenBearer != null) {
            String token = tokenBearer.substring("Bearer".length()).trim();

            byte[] encodedSecret = Base64.getEncoder().encode(secret.getBytes());
            try {
                Algorithm algorithm = Algorithm.HMAC256(encodedSecret);
                JWTVerifier verifier = JWT.require(algorithm)
                        .acceptExpiresAt(0)
                        .build();
                DecodedJWT jwt = verifier.verify(token);

                return true;
            } catch (JWTVerificationException exception){
                LOGGER.error("Token Error: " + exception.getMessage());
                return false;
            }
        }else {
            LOGGER.error("Token is NULL");
            return false;
        }
    }
}
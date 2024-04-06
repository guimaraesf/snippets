package br.com.bs.ratelimiting.service;

public interface RateLimitingLoginService {

    boolean isEnabled();

    Boolean isBlockedIP(String ip);

    void incrementFailedRequestsByIP(String ip);
}

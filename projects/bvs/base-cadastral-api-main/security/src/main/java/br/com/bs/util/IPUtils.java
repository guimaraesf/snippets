package br.com.bs.util;

public class IPUtils {

    private IPUtils() {

    }

    public static String getClientIPFromXForwardedFor(String xForwardedForHeader) {
        if (xForwardedForHeader != null) {
            String[] ips = xForwardedForHeader.split(",");
            if (ips.length > 0) {
                String ip = ips[0];
                if (ip != null && ip.length() != 0 && !"unknown".equalsIgnoreCase(ip)) {
                    return ip;
                }
            }
        }
        return null;
    }
}

package br.com.bs.rest.teste;

import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TesteRest {

    private static final RestTemplate restTemplate = new RestTemplate();
    private static final List<String> cpfs = new ArrayList<String>() {{
        add("00004591478971");
        add("00004141395188");
        add("00070817316191");
        add("00008074111750");
        add("00011584762721");
        add("00096885769649");
        add("00079219985187");
        add("00004798821691");
        add("00000940374773");
        add("00029076263884");
        add("00070899282253");
        add("00001044390921");
        add("00061586633015");
        add("00098983636572");
        add("00034296359851");
        add("00023027074865");
        add("00002811906231");
        add("00000460225383");
        add("00013686382792");
        add("00012958638634");
        add("00092226396500");
        add("00009928526680");
        add("00007218068723");
        add("00037689893805");
        add("00008690598669");
        add("00002116094550");
        add("00039478377809");
        add("00003381258508");
        add("00018397095888");
        add("00060377638366");
        add("00007471431709");
        add("00029038762828");
        add("00046428283972");
        add("00006978921408");
        add("00004600449665");
        add("00040784764816");
        add("00032084780896");
        add("00028563933817");
        add("00029534208000");
        add("00092296548504");
        add("00034897584809");
        add("00010143709739");
    }};

    private static final String baseUrl = "http://localhost:3333/api/v1/";
    private static final List<String> urls = new ArrayList<String>() {{
        add(baseUrl + "identificacao/cpf/");
        add(baseUrl + "contato/cpf/");
        add(baseUrl + "documento/cpf/");
        add(baseUrl + "endereco/cpf/");
    }};

    public static void main(String args[]) throws InterruptedException {
        int contator = 2;
        Random rand = new Random();
        do {
            String cpf = cpfs.get(rand.nextInt(cpfs.size()));
            String url = urls.get(rand.nextInt(urls.size())) + cpf;
            makeCall(url);
            Thread.sleep(1000);
            contator--;
        } while (contator != 0);
    }

    private static void makeCall(String url) {
        try{
            ResponseEntity<String> result = restTemplate.getForEntity(new URI(url), String.class);
            final int statusCode = result.getStatusCodeValue();
            String log = String.format("Status: %s Size: %d", statusCode, result.getBody().length());
            if(statusCode != 200 ||
                    result.getBody().contains("erro_mensagem") ||
                    result.getBody().contains("Exception"))
                System.err.println(log + "\t" + result);
            //else System.out.println(log);
        }catch (Exception e){
            System.err.println("Exception: " + url);
            e.printStackTrace();
        }

    }
}

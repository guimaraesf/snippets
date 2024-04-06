package br.com.bs.oauth2.config;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
public class MappingController {

    @RequestMapping("/oauth/authorize")
    public String authorize() {
        return "forward:/api/oauth/dialog";
    }
}
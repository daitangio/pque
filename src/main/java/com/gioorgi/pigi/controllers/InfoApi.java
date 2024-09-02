package com.gioorgi.pigi.controllers;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class InfoApi {
    
    @RequestMapping("/v1/info")
    public ResponseEntity<String> info(){
        return  ResponseEntity.ok("PIGI ok");
    }
}

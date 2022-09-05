package com.dk.as.agent.controller;


import com.dk.as.agent.service.TestService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RequiredArgsConstructor
@RestController
public class TestApiContoller {

    private final TestService testService;

    Logger logger = LoggerFactory.getLogger(TestApiContoller.class);

    // Like, BW6 Http Receiver,Is it possible to receive all request with one controller.
    @GetMapping("/{tableName}")
    public String getAllGET(@PathVariable String tableName, @RequestParam(required = false)Map<String, String> allParams){
        logger.info("Receive All Search Table Name : {}, Search Condition : {}", tableName, String.valueOf(allParams.entrySet()));
        return "Receive All";
    }

    // using query-parameter, present search condition.
    @GetMapping("/tableName")
    public String searchTableName(@RequestParam(required = false)Map<String, String> allParams){
        logger.info("Controller {}",String.valueOf(allParams.entrySet()));

        return testService.testService(allParams);
    }

    @GetMapping("/tableName2")
    public String searchTableName2(@RequestParam(required = false)Map<String, String> allParams){
        logger.info("Controller2 {}",String.valueOf(allParams.entrySet()));

        return testService.testService(allParams);
    }

}

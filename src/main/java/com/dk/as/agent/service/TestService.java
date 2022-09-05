package com.dk.as.agent.service;


import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.util.Map;

@RequiredArgsConstructor
@Service
public class TestService {

    Logger logger = LoggerFactory.getLogger(TestService.class);

    @Transactional
    public String testService(Map<String, String> requestParams){
        logger.info("Service {}",String.valueOf(requestParams.entrySet()));

        return "Service Success";
    }
}

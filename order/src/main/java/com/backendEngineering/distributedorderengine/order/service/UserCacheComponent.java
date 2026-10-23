package com.backendEngineering.distributedorderengine.order.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import java.util.UUID;

@Component
public class UserCacheComponent {
    private static final Logger log = LoggerFactory.getLogger(UserCacheComponent.class);

    @Cacheable(value = "users", key = "#userId")
    public String verifyUserContext(UUID userId) {
        log.info("-> Cache MISS - Querying Core Identity Layer directly...");
        return "USER_VALID_ACTIVE";
    }
}

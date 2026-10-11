package com.backendEngineering.distributedorderengine.order.config;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.util.Collections;

@Component
@RequiredArgsConstructor
public class RateLimiterFilter implements Filter {
    private final StringRedisTemplate redisTemplate;

    private static final String LUA_SCRIPT =
            "local key = KEYS[1] " +
                    "local limit = tonumber(ARGV[1]) " +
                    "local current = tonumber(redis.call('get', key) or '0') " +
                    "if current + 1 > limit then " +
                    "  return 0 " +
                    "else " +
                    "  redis.call('INCRBY', key, 1) " +
                    "  if current == 0 then " +
                    "    redis.call('EXPIRE', key, 1) " +
                    "  end " +
                    "  return 1 " +
                    "end";

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) req;
        HttpServletResponse response = (HttpServletResponse) res;
        String clientIp = request.getRemoteAddr();

        DefaultRedisScript<Long> script = new DefaultRedisScript<>(LUA_SCRIPT, Long.class);
        Long result = redisTemplate.execute(script, Collections.singletonList("rate:limit:" + clientIp), "5");

        if (result == null || result == 0) {
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"Too Many Requests\",\"message\":\"API Rate limit exceeded.\"}");
            return;
        }
        chain.doFilter(req, res);
    }
}

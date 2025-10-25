package com.airfree.router.tools;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.http.HttpMethod;
import org.springframework.http.server.PathContainer;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * 带缓存的路由匹配器
 */
@Slf4j
@Component
public class AirRouteMatcher {

    private final PathPatternParser pathPatternParser = new PathPatternParser();
    private final Map<String, PathPattern> pathPatternCache = new ConcurrentHashMap<>();
    private final Map<String, Pattern> regexPatternCache = new ConcurrentHashMap<>();

    /**
     * 匹配路由
     */
    public boolean matches(RouteDefinition route, String path, ServerWebExchange exchange) {
        if (route.getPredicates() == null) {
            return false;
        }

        return route.getPredicates().stream()
                .allMatch(predicate -> matchPredicate(predicate, path, exchange));
    }

    /**
     * 匹配单个断言
     */
    private boolean matchPredicate(PredicateDefinition predicate,
                                   String path,
                                   ServerWebExchange exchange) {
        String name = predicate.getName().toLowerCase();
        Map<String, String> args = predicate.getArgs();

        switch (name) {
            case "path":
                return matchPath(args, path);
            case "host":
                return matchHost(args, exchange);
            case "method":
                return matchMethod(args, exchange);
                //todo 这两个先不搞，以后再弄明白
//            case "header":
//                return matchHeader(args, exchange);
//            case "query":
//                return matchQuery(args, exchange);
            default:
                log.debug("不检查的断言类型: {}", name);
                return true; // 对于不支持的断言类型，返回 true 表示不阻止匹配
        }
    }

    /**
     * 路径匹配（带缓存）
     */
    private boolean matchPath(Map<String, String> args, String path) {
        String pattern = getPattern(args);
        if (pattern == null) {
            return false;
        }

        try {
            PathPattern pathPattern = pathPatternCache.computeIfAbsent(pattern,
                    key -> pathPatternParser.parse(key));
            return pathPattern.matches((PathContainer) pathPatternParser.parse(path));
        } catch (Exception e) {
            log.warn("路径模式解析失败: {}", pattern, e);
            return false;
        }
    }

    /**
     * 主机名匹配
     */
    private boolean matchHost(Map<String, String> args, ServerWebExchange exchange) {
        String pattern = getPattern(args);
        if (pattern == null) {
            return true; // 没有配置 Host 断言，允许所有
        }

        String host = exchange.getRequest().getHeaders().getFirst("Host");
        if (host == null) {
            return false;
        }

        Pattern regex = regexPatternCache.computeIfAbsent(pattern,
                key -> compileHostPattern(key));
        return regex.matcher(host).matches();
    }

    /**
     * 编译主机名模式
     */
    private Pattern compileHostPattern(String pattern) {
        // 将 Ant 风格模式转换为正则表达式
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return Pattern.compile(regex);
    }

    /**
     * HTTP 方法匹配
     */
    private boolean matchMethod(Map<String, String> args, ServerWebExchange exchange) {
        String method = args.get("method");
        if (method == null) {
            method = args.get("_genkey_0");
        }

        if (method == null) {
            return true; // 没有配置 Method 断言，允许所有
        }

        HttpMethod requestMethod = exchange.getRequest().getMethod();
        if (requestMethod == null) {
            return false;
        }

        return method.equalsIgnoreCase(requestMethod.name());
    }

    /**
     * 获取模式字符串
     */
    private String getPattern(Map<String, String> args) {
        String pattern = args.get("pattern");
        if (pattern == null) {
            pattern = args.get("_genkey_0");
        }
        return pattern;
    }
}

package com.airfree.aspect;

import com.airfree.annotation.log.AirGatewayLogAnnotation;
import com.airfree.log.core.AirGatewayLogPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * 日志切面
 * 位置：在业务层，拦截特定的方法执行。
 * 关注点：主要关注业务方法的执行，如方法调用参数、返回值、异常、执行时间等。
 * 职责：记录业务日志（Business Log），用于跟踪业务逻辑的执行情况，便于调试和审计。*
 */
@Slf4j
@Aspect
@Component
public class AirGatewayLogAspect {

    private final AirGatewayLogPublisher logPublisher;
    //todo 这个先不通过构造方法注入，先使用自己new出来CompositeMeterRegistry()搞一下
    private final MeterRegistry meterRegistry = new CompositeMeterRegistry();

    public AirGatewayLogAspect(AirGatewayLogPublisher logPublisher) {
        this.logPublisher = logPublisher;
//        this.meterRegistry = meterRegistry;
        log.info("AirGatewayLogAspect初始化完成");
    }

    /**
     * 环绕通知 - 处理同步方法
     */
    @Around("@annotation(airGatewayLogAnnotation)")
    public Object aroundAnnotatedMethod(ProceedingJoinPoint joinPoint,
                                        AirGatewayLogAnnotation airGatewayLogAnnotation) throws Throwable {
        long startTime = System.currentTimeMillis();
        String methodName = joinPoint.getSignature().getName();
        String className = joinPoint.getTarget().getClass().getSimpleName();
        String traceId = UUID.randomUUID().toString();
        // 获取方法返回类型
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Class<?> returnType = signature.getReturnType();
        log.debug("🎯 开始执行方法: {}.{}, 返回类型: {}", className, methodName, returnType.getSimpleName());
        // 根据返回类型选择处理策略
        try {
            if (Mono.class.isAssignableFrom(returnType)) {
                return handleMonoMethod(joinPoint, airGatewayLogAnnotation, startTime, className, methodName, traceId);
            } else if (Flux.class.isAssignableFrom(returnType)) {
                return handleFluxMethod(joinPoint, airGatewayLogAnnotation, startTime, className, methodName, traceId);
            } else {
                return handleSyncMethod(joinPoint, airGatewayLogAnnotation, startTime, className, methodName, traceId);
            }
        } catch (Throwable throwable) {
            // 处理初始化阶段的异常
            long costTime = System.currentTimeMillis() - startTime;
            logError(className, methodName, costTime, throwable, airGatewayLogAnnotation, traceId);
            throw throwable;
        }
    }

    /**
     * 处理同步方法
     */
    private Object handleSyncMethod(ProceedingJoinPoint joinPoint,
                                    AirGatewayLogAnnotation airGatewayLog,
                                    long startTime,
                                    String className,
                                    String methodName,
                                    String traceId) throws Throwable {
        try {
            // 执行目标方法
            Object result = joinPoint.proceed();
            long costTime = System.currentTimeMillis() - startTime;

            logSuccess(className, methodName, costTime, airGatewayLog, traceId);
            return result;

        } catch (Throwable throwable) {
            long costTime = System.currentTimeMillis() - startTime;
            logError(className, methodName, costTime, throwable, airGatewayLog, traceId);
            throw throwable;
        }
    }

    /**
     * 处理 Mono 返回类型的方法
     */
    private Mono<?> handleMonoMethod(ProceedingJoinPoint joinPoint,
                                     AirGatewayLogAnnotation airGatewayLog,
                                     long startTime,
                                     String className,
                                     String methodName,
                                     String traceId) throws Throwable {

        return Mono.defer(() -> {
                    try {
                        return (Mono<?>) joinPoint.proceed();
                    } catch (Throwable e) {
                        return Mono.error(e);
                    }
                })
                .doOnSuccess(result -> {
                    long costTime = System.currentTimeMillis() - startTime;
                    logSuccess(className, methodName, costTime, airGatewayLog, traceId);
                })
                .doOnError(throwable -> {
                    long costTime = System.currentTimeMillis() - startTime;
                    logError(className, methodName, costTime, throwable, airGatewayLog, traceId);
                });
    }

    /**
     * 处理 Flux 返回类型的方法
     */
    private Flux<?> handleFluxMethod(ProceedingJoinPoint joinPoint,
                                     AirGatewayLogAnnotation airGatewayLog,
                                     long startTime,
                                     String className,
                                     String methodName,
                                     String traceId) throws Throwable {

        return Flux.defer(() -> {
                    try {
                        return (Flux<?>) joinPoint.proceed();
                    } catch (Throwable e) {
                        return Flux.error(e);
                    }
                })
                .doOnComplete(() -> {
                    long costTime = System.currentTimeMillis() - startTime;
                    logSuccess(className, methodName, costTime, airGatewayLog, traceId);
                })
                .doOnError(throwable -> {
                    long costTime = System.currentTimeMillis() - startTime;
                    logError(className, methodName, costTime, throwable, airGatewayLog, traceId);
                });
    }

    /**
     * 记录成功日志
     */
    private void logSuccess(String className, String methodName, long costTime,
                            AirGatewayLogAnnotation airGatewayLogAnnotation, String traceId) {
        try {
            if (airGatewayLogAnnotation.async()) {

                Map<String, Object> logData = new HashMap<>();
                logData.put("costTime", costTime);
                logData.put("result", "success");
                logData.put("type", airGatewayLogAnnotation.type().name()); // 确保调用 name()

                logPublisher.publishBusinessLog(traceId, className, methodName,logData );
            } else {
                logSyncBusinessLog(className, methodName, costTime, "success", airGatewayLogAnnotation);
            }

            recordMetrics(className, methodName, costTime, true);
            log.debug("✅ 方法执行成功: {}.{}, 耗时: {}ms", className, methodName, costTime);

        } catch (Exception e) {
            log.warn("记录成功日志失败: {}.{}, 错误: {}", className, methodName, e.getMessage());
        }
    }

    /**
     * 记录错误日志
     */
    private void logError(String className, String methodName, long costTime,
                          Throwable throwable, AirGatewayLogAnnotation airGatewayLogAnnotation, String traceId) {
        try {
            if (airGatewayLogAnnotation.async()) {
                // ✅ 修复：使用 HashMap
                Map<String, Object> logData = new HashMap<>();
                logData.put("costTime", costTime);
                logData.put("result", "error");
                logData.put("error", throwable.getMessage());
                logData.put("type", airGatewayLogAnnotation.type().name()); // 确保调用 name()
                logPublisher.publishBusinessLog(traceId, className, methodName,logData );
            } else {
                logSyncBusinessLog(className, methodName, costTime, "error", airGatewayLogAnnotation);
            }

            recordMetrics(className, methodName, costTime, false);
            log.error("❌ 方法执行失败: {}.{}, 耗时: {}ms, 错误: {}",
                    className, methodName, costTime, throwable.getMessage(), throwable);

        } catch (Exception e) {
            log.warn("记录错误日志失败: {}.{}, 原始错误: {}, 记录错误: {}",
                    className, methodName, throwable.getMessage(), e.getMessage());
        }
    }

    /**
     * 同步记录业务日志
     */
    private void logSyncBusinessLog(String className, String methodName,
                                    long costTime, String result, AirGatewayLogAnnotation airGatewayLog) {
        String logMessage = String.format("[%s] %s.%s - %s - %dms",
                airGatewayLog.type(), className, methodName, result, costTime);

        switch (airGatewayLog.level()) {
            case DEBUG:
                log.debug(logMessage);
                break;
            case INFO:
                log.info(logMessage);
                break;
            case WARN:
                log.warn(logMessage);
                break;
            case ERROR:
                log.error(logMessage);
                break;
            default:
                log.info(logMessage);
        }
    }

    /**
     * 记录指标
     */
    private void recordMetrics(String className, String methodName, long costTime, boolean success) {
        try {
            String metricName = "gateway.business.method." + (success ? "success" : "error");
            meterRegistry.counter(metricName,
                            "class", className,
                            "method", methodName)
                    .increment();

            meterRegistry.timer("gateway.business.method.duration",
                            "class", className,
                            "method", methodName)
                    .record(costTime, TimeUnit.MILLISECONDS);

        } catch (Exception e) {
            log.warn("记录指标失败: {}", e.getMessage());
        }
    }


    /**
     * 前置通知 - 记录方法入参
     */
    @Before("@annotation(airGatewayLogAnnotation)")
    public void logMethodArgs(JoinPoint joinPoint, AirGatewayLogAnnotation airGatewayLogAnnotation) {
        if (log.isDebugEnabled()) {
            String methodName = joinPoint.getSignature().getName();
            Object[] args = joinPoint.getArgs();

            log.debug("方法 {} 调用参数: {}", methodName, Arrays.toString(args));
        }
    }

    /**
     * 后置通知 - 记录方法返回结果（仅同步方法）
     */
    @AfterReturning(pointcut = "@annotation(airGatewayLogAnnotation)", returning = "result")
    public void logMethodResult(JoinPoint joinPoint, Object result, AirGatewayLogAnnotation airGatewayLogAnnotation) {
        if (log.isDebugEnabled()) {
            String methodName = joinPoint.getSignature().getName();

            // 避免记录过大的结果
            String resultStr = result != null ? result.toString() : "null";
            if (resultStr.length() > 500) {
                resultStr = resultStr.substring(0, 500) + "...(truncated)";
            }

            log.debug("方法 {} 返回结果: {}", methodName, resultStr);
        }
    }


    /**
     * 异常通知 - 专门处理异常
     */
    @AfterThrowing(pointcut = "@annotation(airGatewayLogAnnotation)", throwing = "ex")
    public void logMethodException(JoinPoint joinPoint, Exception ex, AirGatewayLogAnnotation airGatewayLogAnnotation) {
        String methodName = joinPoint.getSignature().getName();
        String className = joinPoint.getTarget().getClass().getSimpleName();

        log.error("方法 {}.{} 执行异常: {}", className, methodName, ex.getMessage(), ex);
    }


}

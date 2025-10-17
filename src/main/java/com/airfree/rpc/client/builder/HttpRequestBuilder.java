package com.airfree.rpc.client.builder;

import io.micrometer.observation.ObservationRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.codec.ClientCodecConfigurer;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.*;
import org.springframework.web.util.UriBuilderFactory;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
@Component
public class HttpRequestBuilder implements WebClient.Builder{

    @Override
    public WebClient.Builder baseUrl(String baseUrl) {
        return null;
    }

    @Override
    public WebClient.Builder defaultUriVariables(Map<String, ?> defaultUriVariables) {
        return null;
    }

    @Override
    public WebClient.Builder uriBuilderFactory(UriBuilderFactory uriBuilderFactory) {
        return null;
    }

    @Override
    public WebClient.Builder defaultHeader(String header, String... values) {
        return null;
    }

    @Override
    public WebClient.Builder defaultHeaders(Consumer<HttpHeaders> headersConsumer) {
        return null;
    }

    @Override
    public WebClient.Builder defaultCookie(String cookie, String... values) {
        return null;
    }

    @Override
    public WebClient.Builder defaultCookies(Consumer<MultiValueMap<String, String>> cookiesConsumer) {
        return null;
    }

    @Override
    public WebClient.Builder defaultRequest(Consumer<WebClient.RequestHeadersSpec<?>> defaultRequest) {
        return null;
    }

    @Override
    public WebClient.Builder defaultStatusHandler(Predicate<HttpStatusCode> statusPredicate, Function<ClientResponse, Mono<? extends Throwable>> exceptionFunction) {
        return null;
    }

    @Override
    public WebClient.Builder filter(ExchangeFilterFunction filter) {
        return null;
    }

    @Override
    public WebClient.Builder filters(Consumer<List<ExchangeFilterFunction>> filtersConsumer) {
        return null;
    }

    @Override
    public WebClient.Builder clientConnector(ClientHttpConnector connector) {
        return null;
    }

    @Override
    public WebClient.Builder codecs(Consumer<ClientCodecConfigurer> configurer) {
        return null;
    }

    @Override
    public WebClient.Builder exchangeStrategies(ExchangeStrategies strategies) {
        return null;
    }

    @Override
    public WebClient.Builder exchangeStrategies(Consumer<ExchangeStrategies.Builder> configurer) {
        return null;
    }

    @Override
    public WebClient.Builder exchangeFunction(ExchangeFunction exchangeFunction) {
        return null;
    }

    @Override
    public WebClient.Builder observationRegistry(ObservationRegistry observationRegistry) {
        return null;
    }

    @Override
    public WebClient.Builder observationConvention(ClientRequestObservationConvention observationConvention) {
        return null;
    }

    @Override
    public WebClient.Builder apply(Consumer<WebClient.Builder> builderConsumer) {
        return null;
    }

    @Override
    public WebClient.Builder clone() {
        return null;
    }

    @Override
    public WebClient build() {
        return null;
    }
}

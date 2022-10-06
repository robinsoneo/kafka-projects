package com.ideas.kafka.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfiguration {

    /**
     * Method that allows to create an ElasticsearchClient.
     *
     * @return An {@link ElasticsearchClient} object
     */
    @Bean
    public ElasticsearchClient createElasticsearchClient() {
        return new ElasticsearchClient(new RestClientTransport(
                RestClient.builder(new HttpHost("localhost", 9200,"http"))
                        .build(), new JacksonJsonpMapper()));
    }

    /**
     * Method that allows to create an ElasticsearchClient with security credentials.
     *
     * @return An {@link ElasticsearchClient} object
     */
    //@Bean
    public ElasticsearchClient createElasticsearchClientSecure() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("username", "password"));
        return new ElasticsearchClient(new RestClientTransport(
                RestClient.builder(new HttpHost("localhost", 9200,"http"))
                        .setHttpClientConfigCallback(httpAsync -> httpAsync
                                .setDefaultCredentialsProvider(credentialsProvider))
                        .build(), new JacksonJsonpMapper()));
    }

}

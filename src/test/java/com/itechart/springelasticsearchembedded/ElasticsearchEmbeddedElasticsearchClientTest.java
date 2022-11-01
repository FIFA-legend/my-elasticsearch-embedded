package com.itechart.springelasticsearchembedded;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.itechart.springelasticsearchembedded.document.Person;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EnableAutoConfiguration(exclude = ElasticsearchRestClientAutoConfiguration.class)
class ElasticsearchEmbeddedElasticsearchClientTest {

    private static ElasticsearchClient elasticsearchClient;

    @BeforeAll
    public static void setUp() {
        EmbeddedElastic.node();

        RestClient client = RestClient.builder(
                new HttpHost("localhost", 9200)
        ).build();

        ElasticsearchTransport transport = new RestClientTransport(
                client,
                new JacksonJsonpMapper()
        );

        elasticsearchClient = new ElasticsearchClient(transport);
    }

    @Test
    public void elasticsearchSaveTest() throws InterruptedException {
        String id = "person-id";
        String name = "person-name";
        Person person = new Person();
        person.setId(id);
        person.setName(name);

        String index = "person";

        try {
            IndexResponse response = elasticsearchClient.index(i -> i
                    .index(index)
                    .id(id)
                    .document(person)
            );

            assertThat(response.index()).isEqualTo(index);
            assertThat(response.id()).isEqualTo(id);

            Thread.sleep(2000);

            SearchResponse<Person> search = elasticsearchClient.search(s -> s
                            .index(index)
                            .query(q -> q
                                    .match(t -> t
                                            .field("id")
                                            .query(id)
                                    )
                            ),
                    Person.class);

            assertThat(search.hits().hits().size()).isEqualTo(1);
            for (Hit<Person> hit: search.hits().hits()) {
                assertThat(person).isEqualTo(hit.source());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void shutdown() {
        elasticsearchClient.shutdown();
    }

}

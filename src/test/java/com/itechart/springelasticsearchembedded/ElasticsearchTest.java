package com.itechart.springelasticsearchembedded;

import static org.assertj.core.api.Assertions.assertThat;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.itechart.springelasticsearchembedded.document.Person;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.node.Node;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
@EnableAutoConfiguration(exclude = ElasticsearchRestClientAutoConfiguration.class)
public class ElasticsearchTest {

    private static ElasticsearchClient elasticsearchClient;

    @BeforeAll
    public static void setUp() {
        Node node = EmbeddedElastic.node();
        elasticsearchClient = elasticsearchClient();
    }

    @AfterAll
    public static void shutdown() {
        elasticsearchClient.shutdown();
    }

    private static ElasticsearchClient elasticsearchClient() {
        RestClient client = RestClient.builder(
            new HttpHost("localhost", 9200)
        ).build();

        ElasticsearchTransport transport = new RestClientTransport(
            client,
            new JacksonJsonpMapper()
        );

        return new ElasticsearchClient(transport);
    }

    @Test
    public void elasticsearchClientTest() throws InterruptedException {
        String id = "person-id";
        String name = "person-name";
        Person person = new Person();
        person.setId(id);
        person.setName(name);

        String index = "persons-2";

        try {
            co.elastic.clients.elasticsearch.core.IndexResponse response = elasticsearchClient.index(i -> i
                .index(index)
                .id(id)
                .document(person)
            );

            assertThat(response.index()).isEqualTo(index);
            assertThat(response.id()).isEqualTo(id);

            Thread.sleep(2000);

            co.elastic.clients.elasticsearch.core.SearchResponse<Person> search = elasticsearchClient.search(s -> s
                    .index(index)
                    .query(q -> q
                        .match(t -> t
                            .field("id")
                            .query(id)
                        )
                    ),
                Person.class);

            assertThat(search.hits().hits().size()).isEqualTo(1);
            for (Hit<Person> hit : search.hits().hits()) {
                assertThat(person).isEqualTo(hit.source());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

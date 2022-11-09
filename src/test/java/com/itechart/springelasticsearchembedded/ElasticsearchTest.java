package com.itechart.springelasticsearchembedded;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itechart.springelasticsearchembedded.document.Person;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
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
public class ElasticsearchTest {

    private static Client embeddedClient;

    private static ElasticsearchClient elasticsearchClient;

    @BeforeAll
    public static void setUp() {
        Node node = EmbeddedElastic.node();
        embeddedClient = node.client();
        elasticsearchClient = elasticsearchClient();
    }

    @Test
    void embeddedClientTest() throws IOException {
        String id = "person-id";
        String name = "person-name";
        Person person = new Person();
        person.setId(id);
        person.setName(name);

        String index = "persons-1";

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("id", id);
            builder.field("name", name);
        }
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest(index)
                .id(id)
                .source(builder);

        IndexResponse indexResponse = embeddedClient.index(indexRequest).actionGet();
        assertThat(indexResponse.getResult()).isEqualTo(DocWriteResponse.Result.CREATED);

        embeddedClient.admin().indices().prepareRefresh().get();

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = embeddedClient.search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value).isEqualTo(1);

        final ObjectMapper mapper = new ObjectMapper();
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(person).isEqualTo(mapper.readValue(hit.getSourceAsString(), Person.class));
        }
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
            for (Hit<Person> hit: search.hits().hits()) {
                assertThat(person).isEqualTo(hit.source());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void shutdown() {
        embeddedClient.close();
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

}

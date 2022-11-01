package com.itechart.springelasticsearchembedded;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.itechart.springelasticsearchembedded.document.Person;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.action.DocWriteResponse.Result;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EnableAutoConfiguration(exclude = ElasticsearchRestClientAutoConfiguration.class)
public class ElasticsearchEmbeddedClientTest {

    private static Client client;

    @BeforeAll
    public static void setUp() {
        Node node = EmbeddedElastic.node();
        client = node.client();
    }

    @Test
    void test() throws IOException {
        String id = "person-id";
        String name = "person-name";
        Person person = new Person();
        person.setId(id);
        person.setName(name);

        String index = "persons";

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

        IndexResponse indexResponse = client.index(indexRequest).actionGet();
        assertThat(indexResponse.getResult()).isEqualTo(Result.CREATED);

        client.admin().indices().prepareRefresh().get();

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value).isEqualTo(1);

        final ObjectMapper mapper = new ObjectMapper();
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(person).isEqualTo(mapper.readValue(hit.getSourceAsString(), Person.class));
        }
    }

    @AfterAll
    public static void shutdown() {
        client.close();
    }

}

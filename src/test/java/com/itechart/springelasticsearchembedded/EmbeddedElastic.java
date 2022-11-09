package com.itechart.springelasticsearchembedded;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.PluginsService;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class EmbeddedElastic {

    private static final int PORT = 9200;

    public static Node node() {
        String tempDir = new File("elastic_search_temp").getAbsoluteFile().toString();
        log.info("Created tempt dir: {}", tempDir);

        Map<String, String> settings = new HashMap<>();
        settings.put("cluster.name", new ClusterName("single-node-cluster").value());
        settings.put("node.name", "node");
        settings.put("path.home", tempDir);
        settings.put("path.repo", tempDir + File.separator + "repo");
        settings.put("path.data", tempDir + File.separator + "data");
        settings.put("path.logs", tempDir + File.separator + "logs");
        settings.put("path.shared_data", tempDir + File.separator + "shared");
        settings.put("http.port", Integer.toString(PORT));
        settings.put("transport.type", "netty4");
        settings.put("discovery.seed_hosts", "0.0.0.0");
        settings.put(NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.getKey(), "_local_");
        settings.put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE);
        log.info("Starting Elastic Node");
        final Node node = new PluginNode(settings);
        try {
            node.start();
        } catch (NodeValidationException e) {
            throw new RuntimeException(e);
        }
        log.info("Started Elastic Node");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping Elastic Node");
            try {
                node.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.info("Stopped Elastic Node");

            log.info("Deleting temp dir: {}", tempDir);
            try {
                FileUtils.forceDeleteOnExit(new File(tempDir));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));

        return node;
    }

    private static class PluginNode extends Node {
        public PluginNode(Map<String, String> preparedSettings) {
            super(
                    InternalSettingsPreparer.prepareEnvironment(Settings.EMPTY, preparedSettings, null, () -> "node-test"),
                    settings -> {
                        URL resource = EmbeddedElastic.class.getClassLoader().getResource("elasticsearch/plugins");
                        Path path = null;
                        try {
                            if (resource != null) path = Paths.get(resource.toURI());
                        } catch (URISyntaxException e) {
                            throw new RuntimeException(e);
                        }
                        return new PluginsService(
                                settings,
                                null,
                                null,
                                path
                        );
                    },
                    false
            );
            log.info("Started local elastic");
        }
    }
}


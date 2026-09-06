package com.ritikbansod.kafkawrapper.connection;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Manage cluster connections at runtime: create/edit profiles with PLAINTEXT, mTLS
 * or SASL/OAUTHBEARER settings, test them, reconnect. Secrets are never returned
 * back to the UI — they are masked.
 */
@RestController
@RequestMapping("/api/clusters")
public class ConnectionController {

    private final ConnectionStore store;
    private final KafkaClusterManager manager;

    public ConnectionController(ConnectionStore store, KafkaClusterManager manager) {
        this.store = store;
        this.manager = manager;
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        List<Map<String, Object>> result = new ArrayList<>();
        result.add(view(manager.profileOf(KafkaClusterManager.DEFAULT_CLUSTER_ID), true, null));
        store.all().forEach(profile -> result.add(view(profile, false, null)));
        return result;
    }

    @PostMapping("/test")
    public Map<String, Object> test(@RequestBody ConnectionProfile profile) {
        KafkaClusterManager.TestResult result = manager.test(profile);
        return testView(result);
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody ConnectionProfile profile) {
        ConnectionProfile saved = store.save(profile);
        manager.evict(saved.id());
        return ResponseEntity.status(HttpStatus.CREATED).body(view(saved, false, null));
    }

    @PutMapping("/{id}")
    public ResponseEntity<Map<String, Object>> update(@PathVariable String id, @RequestBody ConnectionProfile profile) {
        ConnectionProfile existing = store.find(id)
                .orElseThrow(() -> new NoSuchElementException("Unknown cluster id '" + id + "'"));
        ConnectionProfile merged = new ConnectionProfile(id, profile.name(), profile.bootstrapServers(),
                mergeSecrets(profile.security(), existing.security()));
        ConnectionProfile saved = store.save(merged);
        manager.evict(id);
        return ResponseEntity.ok(view(saved, false, null));
    }

    @DeleteMapping("/{id}")
    public Map<String, Object> delete(@PathVariable String id) {
        if (!store.delete(id)) {
            throw new NoSuchElementException("Unknown cluster id '" + id + "'");
        }
        manager.evict(id);
        return Map.of("id", id, "status", "deleted");
    }

    @PostMapping("/{id}/reconnect")
    public Map<String, Object> reconnect(@PathVariable String id) {
        manager.evict(id);
        manager.get(id); // force a fresh handle
        return Map.of("id", id, "status", "reconnected");
    }

    /**
     * The UI sends masked secrets ("••••••") back on edit; replace them with the stored values.
     */
    private SecuritySettings mergeSecrets(SecuritySettings incoming, SecuritySettings stored) {
        if (incoming == null) {
            return null;
        }
        return new SecuritySettings(
                incoming.protocol(), incoming.saslMechanism(),
                incoming.saslUsername(), unmask(incoming.saslPassword(), stored.saslPassword()),
                incoming.oauthTokenUrl(), incoming.oauthClientId(),
                unmask(incoming.oauthClientSecret(), stored.oauthClientSecret()), incoming.oauthScope(),
                incoming.keystoreLocation(), unmask(incoming.keystorePassword(), stored.keystorePassword()),
                incoming.keystoreType(),
                incoming.truststoreLocation(), unmask(incoming.truststorePassword(), stored.truststorePassword()),
                incoming.truststoreType(),
                isConfiguredMarker(incoming.keystoreCertificateChainPem())
                        ? stored.keystoreCertificateChainPem() : incoming.keystoreCertificateChainPem(),
                isConfiguredMarker(incoming.keystoreKeyPem())
                        ? stored.keystoreKeyPem() : incoming.keystoreKeyPem(),
                isConfiguredMarker(incoming.truststoreCertificatesPem())
                        ? stored.truststoreCertificatesPem() : incoming.truststoreCertificatesPem(),
                incoming.endpointVerificationEnabled());
    }

    private static boolean isConfiguredMarker(String value) {
        return SecuritySettings.maskedMarker.equals(value);
    }

    private static String unmask(String incoming, String stored) {
        return SecuritySettings.maskedMarker.equals(incoming) && stored != null ? stored : incoming;
    }

    private Map<String, Object> view(ConnectionProfile profile, boolean builtIn, String activeClusterId) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("id", profile.id());
        view.put("name", profile.displayName());
        view.put("bootstrapServers", profile.bootstrapServers());
        view.put("security", profile.security() == null ? SecuritySettings.plaintext().masked() : profile.security().masked());
        view.put("builtIn", builtIn);
        return view;
    }

    private Map<String, Object> testView(KafkaClusterManager.TestResult result) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("ok", result.ok());
        if (result.ok()) {
            view.put("clusterId", result.clusterId());
            view.put("nodes", result.nodes());
        } else {
            view.put("error", result.error());
        }
        return view;
    }
}

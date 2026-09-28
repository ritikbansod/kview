package com.ritikbansod.kview.web;

import com.ritikbansod.kview.auth.AuthProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Public runtime info for clients: the UI uses it to show the read-only banner,
 * scripts can detect the auth mode before hitting protected endpoints.
 */
@RestController
public class MetaController {

    private final AuthProperties auth;
    private final boolean readonly;

    public MetaController(AuthProperties auth,
                          @Value("${kview.readonly:false}") boolean readonly) {
        this.auth = auth;
        this.readonly = readonly;
    }

    @GetMapping("/api/meta")
    public Map<String, Object> meta() {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("readonly", readonly);
        meta.put("authMode", auth.mode());
        return meta;
    }
}

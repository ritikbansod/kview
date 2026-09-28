package com.ritikbansod.kview.web;

import org.springframework.context.annotation.Configuration;

/**
 * CORS lives in {@link com.ritikbansod.kview.auth.SecurityConfig}, which
 * keeps the permissive local-development setting only while the API is
 * unauthenticated (kview.auth.mode=none) and closes it otherwise.
 */
@Configuration
public class WebConfig {
}

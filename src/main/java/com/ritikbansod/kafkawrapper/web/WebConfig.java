package com.ritikbansod.kafkawrapper.web;

import org.springframework.context.annotation.Configuration;

/**
 * CORS lives in {@link com.ritikbansod.kafkawrapper.auth.SecurityConfig}, which
 * keeps the permissive local-development setting only while the API is
 * unauthenticated (kview.auth.mode=none) and closes it otherwise.
 */
@Configuration
public class WebConfig {
}

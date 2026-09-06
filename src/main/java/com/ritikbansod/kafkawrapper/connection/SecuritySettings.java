package com.ritikbansod.kafkawrapper.connection;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Security settings for a cluster connection. Everything is configurable from the UI:
 * <ul>
 *   <li>PLAINTEXT — no auth</li>
 *   <li>SSL — TLS with optional client certificate (mTLS) via keystore files or pasted PEM</li>
 *   <li>SASL_PLAINTEXT / SASL_SSL with SASL PLAIN, SCRAM-SHA-256/512, or OAUTHBEARER</li>
 * </ul>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SecuritySettings(
        String protocol,
        String saslMechanism,
        String saslUsername,
        String saslPassword,
        // OAUTHBEARER (client credentials grant)
        String oauthTokenUrl,
        String oauthClientId,
        String oauthClientSecret,
        String oauthScope,
        // TLS — file based
        String keystoreLocation,
        String keystorePassword,
        String keystoreType,
        String truststoreLocation,
        String truststorePassword,
        String truststoreType,
        // TLS — pasted PEM (stored with the profile)
        String keystoreCertificateChainPem,
        String keystoreKeyPem,
        String truststoreCertificatesPem,
        // null/true => standard hostname verification; false => disabled
        Boolean endpointVerificationEnabled) {

    public static final String PLAINTEXT = "PLAINTEXT";
    public static final String SSL = "SSL";
    public static final String SASL_PLAINTEXT = "SASL_PLAINTEXT";
    public static final String SASL_SSL = "SASL_SSL";

    /** Marker returned instead of secrets; on edit it means "keep the stored value". */
    public static final String maskedMarker = "••••••";

    public static SecuritySettings plaintext() {
        return new SecuritySettings(PLAINTEXT, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, null);
    }

    public boolean usesTls() {
        return SSL.equals(protocol()) || SASL_SSL.equals(protocol());
    }

    public boolean usesSasl() {
        String p = protocol();
        return SASL_PLAINTEXT.equals(p) || SASL_SSL.equals(p);
    }

    public String protocolOrDefault() {
        return protocol == null || protocol.isBlank() ? PLAINTEXT : protocol;
    }

    public String saslMechanismOrDefault() {
        return saslMechanism == null || saslMechanism.isBlank() ? "PLAIN" : saslMechanism;
    }

    public boolean endpointVerification() {
        return endpointVerificationEnabled == null || endpointVerificationEnabled;
    }

    /** Masked copy safe to return from list/test endpoints — secrets replaced with fixed markers. */
    public SecuritySettings masked() {
        return new SecuritySettings(protocol, saslMechanism,
                saslUsername, mask(saslPassword),
                oauthTokenUrl, oauthClientId, mask(oauthClientSecret), oauthScope,
                keystoreLocation, mask(keystorePassword), keystoreType,
                truststoreLocation, mask(truststorePassword), truststoreType,
                present(keystoreCertificateChainPem), present(keystoreKeyPem), present(truststoreCertificatesPem),
                endpointVerificationEnabled);
    }

    private static String mask(String secret) {
        return secret == null || secret.isBlank() ? null : maskedMarker;
    }

    private static String present(String pem) {
        return pem == null || pem.isBlank() ? null : "configured";
    }
}

package com.ritikbansod.kafkawrapper.connection;

import com.ritikbansod.kafkawrapper.connection.oauth.HttpOAuthBearerLoginCallbackHandler;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;

import java.util.HashMap;
import java.util.Map;

/**
 * Translates a {@link ConnectionProfile} into raw Kafka client configuration.
 * One profile -> admin/producer/consumer all speak the same security.
 */
public final class KafkaClientPropertiesFactory {

    public static final String SSL_KEYSTORE_CERT_CHAIN = "ssl.keystore.certificate.chain";
    public static final String SSL_KEYSTORE_KEY = "ssl.keystore.key";
    public static final String SSL_TRUSTSTORE_CERTS = "ssl.truststore.certificates";

    private KafkaClientPropertiesFactory() {
    }

    public static Map<String, Object> create(ConnectionProfile profile) {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", profile.bootstrapServers()));

        SecuritySettings security = profile.security();
        String protocol = security == null ? SecuritySettings.PLAINTEXT : security.protocolOrDefault();
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);

        if (security != null) {
            if (security.usesSasl()) {
                applySasl(props, security);
            }
            if (security.usesTls()) {
                applyTls(props, security);
            }
        }
        return props;
    }

    static void applySasl(Map<String, Object> props, SecuritySettings security) {
        String mechanism = security.saslMechanismOrDefault();
        props.put(SaslConfigs.SASL_MECHANISM, mechanism);
        switch (mechanism) {
            case "PLAIN" -> props.put(SaslConfigs.SASL_JAAS_CONFIG, jaas(
                    PlainLoginModule.class.getName(),
                    "username", text(security.saslUsername()),
                    "password", text(security.saslPassword())));
            case "SCRAM-SHA-256", "SCRAM-SHA-512" -> props.put(SaslConfigs.SASL_JAAS_CONFIG, jaas(
                    ScramLoginModule.class.getName(),
                    "username", text(security.saslUsername()),
                    "password", text(security.saslPassword())));
            case "OAUTHBEARER" -> {
                props.put(SaslConfigs.SASL_JAAS_CONFIG, jaas(
                        OAuthBearerLoginModule.class.getName(),
                        TOKEN_URL, text(security.oauthTokenUrl()),
                        CLIENT_ID, text(security.oauthClientId()),
                        CLIENT_SECRET, text(security.oauthClientSecret()),
                        SCOPE, text(security.oauthScope())));
                props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                        HttpOAuthBearerLoginCallbackHandler.class.getName());
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported SASL mechanism '" + mechanism + "' (use PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 or OAUTHBEARER)");
        }
    }

    static void applyTls(Map<String, Object> props, SecuritySettings security) {
        if (hasText(security.keystoreLocation())) {
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, security.keystoreLocation());
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, text(security.keystorePassword()));
            if (hasText(security.keystoreType())) {
                props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, security.keystoreType());
            }
            if (hasText(security.keystorePassword())) {
                props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, security.keystorePassword());
            }
        } else if (hasText(security.keystoreCertificateChainPem()) && hasText(security.keystoreKeyPem())) {
            // mTLS with pasted PEM material — no files needed on the wrapper host
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
            props.put(SSL_KEYSTORE_CERT_CHAIN, security.keystoreCertificateChainPem());
            props.put(SSL_KEYSTORE_KEY, security.keystoreKeyPem());
            if (hasText(security.keystorePassword())) {
                props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, security.keystorePassword());
            }
        }

        if (hasText(security.truststoreLocation())) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, security.truststoreLocation());
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, text(security.truststorePassword()));
            if (hasText(security.truststoreType())) {
                props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, security.truststoreType());
            }
        } else if (hasText(security.truststoreCertificatesPem())) {
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
            props.put(SSL_TRUSTSTORE_CERTS, security.truststoreCertificatesPem());
        }

        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                security.endpointVerification() ? "https" : "");
    }

    public static String jaas(String loginModule, String... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("JAAS options must be key/value pairs");
        }
        StringBuilder jaas = new StringBuilder(loginModule).append(" required");
        for (int i = 0; i < keyValues.length; i += 2) {
            jaas.append(' ').append(keyValues[i]).append("=\"").append(escape(keyValues[i + 1])).append('"');
        }
        return jaas.append(';').toString();
    }

    static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static final String TOKEN_URL = "tokenUrl";
    private static final String CLIENT_ID = "clientId";
    private static final String CLIENT_SECRET = "clientSecret";
    private static final String SCOPE = "scope";

    private static String text(String value) {
        return value == null ? "" : value;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}

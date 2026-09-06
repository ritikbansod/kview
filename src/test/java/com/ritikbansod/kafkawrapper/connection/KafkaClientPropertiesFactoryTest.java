package com.ritikbansod.kafkawrapper.connection;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaClientPropertiesFactoryTest {

    @Test
    void plaintextIsTheDefault() {
        ConnectionProfile profile = new ConnectionProfile("id", "local", List.of("localhost:9092"), null);
        Map<String, Object> props = KafkaClientPropertiesFactory.create(profile);
        assertThat(props.get("bootstrap.servers")).isEqualTo("localhost:9092");
        assertThat(props.get("security.protocol")).isEqualTo("PLAINTEXT");
    }

    @Test
    void scramOverSslWithKeystores() {
        SecuritySettings security = new SecuritySettings("SASL_SSL", "SCRAM-SHA-512", "admin", "s3cret",
                null, null, null, null,
                "/certs/keystore.p12", "storepass", "PKCS12",
                "/certs/truststore.p12", "trustpass", "PKCS12",
                null, null, null, true);
        Map<String, Object> props = KafkaClientPropertiesFactory.create(
                new ConnectionProfile("id", "prod", List.of("broker:9093"), security));

        assertThat(props.get("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(props.get("sasl.mechanism")).isEqualTo("SCRAM-SHA-512");
        String jaas = (String) props.get("sasl.jaas.config");
        assertThat(jaas).startsWith("org.apache.kafka.common.security.scram.ScramLoginModule required")
                .contains("username=\"admin\"").contains("password=\"s3cret\"").endsWith(";");
        assertThat(props.get("ssl.keystore.location")).isEqualTo("/certs/keystore.p12");
        assertThat(props.get("ssl.keystore.type")).isEqualTo("PKCS12");
        assertThat(props.get("ssl.truststore.certificates")).isNull();
        assertThat((String) props.get("ssl.endpoint.identification.algorithm")).isEqualTo("https");
    }

    @Test
    void pemCertificatesWithoutFiles() {
        SecuritySettings security = new SecuritySettings("SSL", null, null, null,
                null, null, null, null,
                null, null, null, null, null, null,
                "-----BEGIN CERTIFICATE-----\nCLIENT\n-----END CERTIFICATE-----",
                "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----",
                "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
                false);
        Map<String, Object> props = KafkaClientPropertiesFactory.create(
                new ConnectionProfile("id", "mtls", List.of("broker:9093"), security));

        assertThat(props.get("ssl.keystore.type")).isEqualTo("PEM");
        assertThat((String) props.get("ssl.keystore.certificate.chain")).contains("CLIENT");
        assertThat((String) props.get("ssl.keystore.key")).contains("KEY");
        assertThat(props.get("ssl.truststore.type")).isEqualTo("PEM");
        assertThat((String) props.get("ssl.truststore.certificates")).contains("CA");
        assertThat((String) props.get("ssl.endpoint.identification.algorithm")).isEmpty();
    }

    @Test
    void oauthUsesTheTokenEndpointHandler() {
        SecuritySettings security = new SecuritySettings("SASL_SSL", "OAUTHBEARER", null, null,
                "https://idp.example.com/token", "kview", "top-secret", "kafka",
                null, null, null, null, null, null, null, null, null, true);
        Map<String, Object> props = KafkaClientPropertiesFactory.create(
                new ConnectionProfile("id", "oauth", List.of("broker:9093"), security));

        assertThat(props.get("sasl.mechanism")).isEqualTo("OAUTHBEARER");
        String jaas = (String) props.get("sasl.jaas.config");
        assertThat(jaas).contains("OAuthBearerLoginModule required")
                .contains("tokenUrl=\"https://idp.example.com/token\"")
                .contains("clientId=\"kview\"")
                .contains("clientSecret=\"top-secret\"")
                .contains("scope=\"kafka\"");
        assertThat((String) props.get("sasl.login.callback.handler.class"))
                .endsWith("HttpOAuthBearerLoginCallbackHandler");
    }

    @Test
    void jaasValuesWithQuotesAreEscaped() {
        String jaas = KafkaClientPropertiesFactory.jaas("Module", "password", "pa\"ss\\word");
        assertThat(jaas).isEqualTo("Module required password=\"pa\\\"ss\\\\word\";");
    }

    @Test
    void unknownMechanismIsRejected() {
        SecuritySettings security = new SecuritySettings("SASL_PLAINTEXT", "GSSAPI", "u", "p",
                null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertThatThrownBy(() -> KafkaClientPropertiesFactory.create(
                new ConnectionProfile("id", "x", List.of("b:9092"), security)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("GSSAPI");
    }
}

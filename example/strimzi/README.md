# Ready-to-Deploy Strimzi KRaft Cluster (Docker Kubernetes & Cloud)

This directory contains declarative Kubernetes manifests to deploy an Apache Kafka **4.3.1** cluster using the latest **Strimzi 1.2.0** operator.

It is pre-configured and optimized to run seamlessly on **Docker-based Kubernetes clusters** (Docker Desktop K8s, Kind, Minikube) as well as cloud providers (EKS, GKE, AKS).

---

## Architecture Overview

* **Strimzi Version:** `1.2.0` (Latest)
* **Kafka Version:** `4.3.1` (KRaft Mode)
* **Controllers (KRaft Quorum):** 3 dedicated controllers managed via `KafkaNodePool` (`name: controller`)
* **Brokers (Data Plane):** 3 dedicated brokers managed via `KafkaNodePool` (`name: broker`)
* **Memory Tuned for Docker:** JVM heap sizes (`-Xms`/`-Xmx`) and memory requests/limits are tuned so that all 6 Kafka nodes + Operator + Keycloak fit easily in standard Docker engine limits (4GB–8GB RAM).

```
                      [ Docker Kubernetes Cluster ]
  
  +-------------------------------------------------------------------------+
  |                                                                         |
  |   KafkaNodePool (3 Controllers)         KafkaNodePool (3 Brokers)       |
  |   +---------------------------+         +---------------------------+   |
  |   | kview-cluster-controller-0|         |  kview-cluster-broker-0   |   |
  |   | kview-cluster-controller-1|  <--->  |  kview-cluster-broker-1   |   |
  |   | kview-cluster-controller-2|         |  kview-cluster-broker-2   |   |
  |   +---------------------------+         +-------------+-------------+   |
  |                                                       |                 |
  +-------------------------------------------------------|-----------------+
                                                          |
                 +-------------------+--------------------+-------------------+
                 |                   |                    |                   |
                 v                   v                    v                   v
           [:9092 Internal]    [:9093 Internal]     [:9094 External]    [:9095 External]
             PLAINTEXT           Internal mTLS        OAuth 2.0 / OIDC     External mTLS
             (No Auth)           (Client Cert)       (SASL_SSL OAUTH)      (Client Cert)
```

### Configured Listeners

| Port | Name | Type | Protocol | Auth Mechanism | Intended Use |
|---|---|---|---|---|---|
| **`9092`** | `plain` | `internal` | `PLAINTEXT` | None | Internal unencrypted testing within the cluster |
| **`9093`** | `tls` | `internal` | `SSL` | mTLS (TLS Client Auth) | Secure inter-service communication inside Kubernetes |
| **`9094`** / **`443`** | `oauth` | `loadbalancer` | `SASL_SSL` | OAuth 2.0 (`OAUTHBEARER`) | External authentication against Keycloak / Okta / Entra ID |
| **`9095`** | `extmtls` | `loadbalancer` | `SSL` | mTLS (TLS Client Auth) | External client certificate authentication |

---

## File Structure

- [00-crds.yaml](file:///f:/FREELANCE/kafka-wrapper/example/strimzi/00-crds.yaml): Official Strimzi 1.2.0 CRDs (installed first to prevent schema errors).
- [01-strimzi-operator.yaml](file:///f:/FREELANCE/kafka-wrapper/example/strimzi/01-strimzi-operator.yaml): Strimzi 1.2.0 Cluster Operator configured for namespace `kafka`.
- [02-kafka-cluster.yaml](file:///f:/FREELANCE/kafka-wrapper/example/strimzi/02-kafka-cluster.yaml): Declares 3 controllers, 3 brokers (`KafkaNodePool`), and the 4 listeners.
- [03-sample-user-topic.yaml](file:///f:/FREELANCE/kafka-wrapper/example/strimzi/03-sample-user-topic.yaml): Declares `test-topic` (3 partitions, 3 replicas) and `test-mtls-user` (auto-issues mTLS certs).
- [04-keycloak-oauth-mock.yaml](file:///f:/FREELANCE/kafka-wrapper/example/strimzi/04-keycloak-oauth-mock.yaml): Self-contained Keycloak OIDC provider for out-of-the-box OAuth testing.
- [kind-config.yaml](file:///f:/FREELANCE/kafka-wrapper/example/strimzi/kind-config.yaml): Cluster configuration for Kind (Kubernetes in Docker) with port mappings.
- [kustomization.yaml](file:///f:/FREELANCE/kafka-wrapper/example/strimzi/kustomization.yaml): Allows deploying the full stack with `kubectl apply -k example/strimzi/`.

---

## Quick Start on Docker Kubernetes

### Option A: Using Kind (Kubernetes in Docker)

If you use Kind, create the cluster with the included port mappings:

```bash
kind create cluster --name kview-test --config example/strimzi/kind-config.yaml
```

### Option B: Using Docker Desktop Kubernetes

Ensure Kubernetes is enabled in Docker Desktop Settings (`Settings -> Kubernetes -> Enable Kubernetes`).

### Option C: Using Minikube (Docker Driver)

```bash
minikube start --driver=docker --cpus=4 --memory=6144
# In a separate terminal, enable LoadBalancer routing:
minikube tunnel
```

---

## Deploying the Stack

You can deploy the manifests sequentially (recommended for learning the lifecycle) or in one command via Kustomize:

### Method 1: Sequential Deployment

```bash
# 1. Install CRDs first (registers schemas with the K8s API server)
kubectl apply -f example/strimzi/00-crds.yaml

# 2. Deploy the Strimzi Operator into the 'kafka' namespace
kubectl apply -f example/strimzi/01-strimzi-operator.yaml
kubectl wait deployment/strimzi-cluster-operator -n kafka --for=condition=Available=True --timeout=300s

# 3. (Optional) Deploy Keycloak for testing the OAuth 2.0 listener
kubectl apply -f example/strimzi/04-keycloak-oauth-mock.yaml
kubectl wait deployment/keycloak -n kafka --for=condition=Available=True --timeout=180s

# 4. Deploy the 3-Controller + 3-Broker KRaft Cluster
kubectl apply -f example/strimzi/02-kafka-cluster.yaml
kubectl wait kafka/kview-cluster -n kafka --for=condition=Ready=True --timeout=600s

# 5. Deploy sample topic and mTLS user
kubectl apply -f example/strimzi/03-sample-user-topic.yaml
```

### Method 2: Single-Command Deployment (via Kustomize)

```bash
kubectl apply -k example/strimzi/
```

---

## Verifying the Cluster

```bash
# Check KafkaNodePool resources
kubectl get knp -n kafka
```
Output:
```text
NAME         DESIRED   CURRENT   READY   ROLES
broker       3         3         3       broker
controller   3         3         3       controller
```

```bash
# View all running pods
kubectl get pods -n kafka
```
Output:
```text
NAME                                         READY   STATUS    RESTARTS   AGE
keycloak-xxxxxxxxxx-xxxxx                    1/1     Running   0          3m
kview-cluster-broker-0                       1/1     Running   0          2m
kview-cluster-broker-1                       1/1     Running   0          2m
kview-cluster-broker-2                       1/1     Running   0          2m
kview-cluster-controller-0                   1/1     Running   0          2m
kview-cluster-controller-1                   1/1     Running   0          2m
kview-cluster-controller-2                   1/1     Running   0          2m
kview-cluster-entity-operator-xxxxxxxxxx     2/2     Running   0          1m
strimzi-cluster-operator-xxxxxxxxxx-xxxxx    1/1     Running   0          4m
```

---

## How to Test Each Listener

### 1. Test Port 9092: PLAINTEXT (Internal)

Run a test producer pod directly inside the cluster:
```bash
kubectl run kafka-producer -n kafka --rm -i --tty --image=apache/kafka:4.3.1 -- \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kview-cluster-kafka-bootstrap.kafka.svc:9092 \
  --topic test-topic
```

---

### 2. Test Port 9093: Internal mTLS (Mutual TLS)

Extract the cluster CA certificate and user credentials from the generated Kubernetes secrets:
```bash
# Cluster CA cert
kubectl get secret kview-cluster-cluster-ca-cert -n kafka -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt

# User cert and private key
kubectl get secret test-mtls-user -n kafka -o jsonpath='{.data.user\.crt}' | base64 -d > user.crt
kubectl get secret test-mtls-user -n kafka -o jsonpath='{.data.user\.key}' | base64 -d > user.key
```

Connect inside the cluster using `SSL` protocol with `ca.crt`, `user.crt`, and `user.key`.

---

### 3. Test Port 9094 / 443: External OAuth 2.0 (`OAUTHBEARER`)

#### Step A: Fetch an Access Token from Keycloak
```bash
TOKEN=$(curl -s -X POST "http://localhost:8080/realms/kafka/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=kview-client" \
  -d "client_secret=kview-secret" | jq -r .access_token)
```

#### Step B: Connect Kafka Client
Configure client properties:
```properties
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
ssl.truststore.location=/path/to/ca.crt
ssl.truststore.type=PEM
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.token="<TOKEN>";
```

---

### 4. Test Port 9095: External mTLS

Bootstrap service address:
```bash
kubectl get svc kview-cluster-kafka-extmtls-bootstrap -n kafka
```
*(On Docker Desktop or Kind with port mappings, connect to `localhost:9095`)*.

Connect using:
- **Security Protocol:** `SSL`
- **Truststore / CA:** `ca.crt` (from secret `kview-cluster-cluster-ca-cert`)
- **Client Certificate:** `user.crt` (from secret `test-mtls-user`)
- **Client Key:** `user.key` (from secret `test-mtls-user`)

---

## Connecting Kview

In the **Kview Web UI** (`Connections -> Add cluster`):

1. **PLAINTEXT (Port 9092):**
   - Bootstrap: `kview-cluster-kafka-bootstrap.kafka.svc:9092`
   - Protocol: `PLAINTEXT`

2. **mTLS (Port 9093 / 9095):**
   - Bootstrap: `localhost:9095` (or external IP)
   - Protocol: `SSL`
   - CA Certificate: Paste contents of `ca.crt`
   - Client Certificate: Paste contents of `user.crt`
   - Client Key: Paste contents of `user.key`
   - Hostname Verification: OFF

3. **OAuth 2.0 (Port 9094):**
   - Bootstrap: `localhost:9094`
   - Protocol: `SASL_SSL`
   - SASL Mechanism: `OAUTHBEARER`
   - Token Endpoint URL: `http://localhost:8080/realms/kafka/protocol/openid-connect/token`
   - Client ID: `kview-client`
   - Client Secret: `kview-secret`
   - CA Certificate: Paste contents of `ca.crt`

---

## Teardown

```bash
kubectl delete -f example/strimzi/03-sample-user-topic.yaml
kubectl delete -f example/strimzi/02-kafka-cluster.yaml
kubectl delete -f example/strimzi/04-keycloak-oauth-mock.yaml
kubectl delete pvc -n kafka -l strimzi.io/cluster=kview-cluster
kubectl delete -f example/strimzi/01-strimzi-operator.yaml
kubectl delete -f example/strimzi/00-crds.yaml
```

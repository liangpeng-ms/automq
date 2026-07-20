# AutoMQ broker/controller kustomize split — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the combined multi-node deploy into two separate kustomize-managed services (`automq-broker`, `automq-controller`), each a Falcon `Application` CR base plus a single `magnetar-test` overlay.

**Architecture:** Keep the Falcon `Application` CR wrapper (Service + StatefulSet inlined in `spec.workload[]`). Borrow only MtOnAks' directory form: `deploy/<svc>/kustomize/{bases, magnetar-test}`. Base holds a complete deployable CR; overlay overrides only the container image via a JSON6902 patch (kustomize's `images:` transformer cannot reach an inlined CR).

**Tech Stack:** Kubernetes kustomize (`kustomize.config.k8s.io/v1beta1`), Falcon `apis.clusterfleet.io/v1alpha1` Application CR, AutoMQ `start.sh up`, HDFS/WebHDFS backend. Verification via `registry.k8s.io/kustomize/kustomize` Docker image (no local kustomize binary).

## Global Constraints

- Namespace `magnetar-test` — hardcoded in every CR's manifest metadata (kustomize `namespace:` cannot rewrite inlined CR manifests).
- FileIO FQN MUST be the new `com.automq.hdfs.table.io.WebHdfsFileIO` (the separated source files use the old `kafka.automq.table.io.WebHdfsFileIO` — fix on copy).
- Base image: `msbingmtcr.azurecr.io/rongyu/automq:test0712sr`.
- CR `spec.workload[0]` = Service, `spec.workload[1]` = StatefulSet. JSON6902 image path: `/spec/workload/1/manifest/spec/template/spec/containers/0/image`.
- Do NOT modify `deploy/automq-multi.yaml` or `deploy/automq-single.yaml`.
- Broker: `process.roles=broker`, node.id `100000+ordinal`, 3 replicas, `podManagementPolicy: Parallel`, probes on port `kafka` (9092), Table Topic config appended to `broker.properties`.
- Controller: `process.roles=controller`, node.id `200000+ordinal`, 1 replica, sole voter, probes on port `controller` (9093), no Table Topic config.

---

### Task 1: Broker base

**Files:**
- Create: `deploy/automq-broker/kustomize/bases/application.yaml`
- Create: `deploy/automq-broker/kustomize/bases/kustomization.yaml`

**Interfaces:**
- Produces: an `Application` CR named `automq-broker-pelian` in namespace `magnetar-test`; base kustomization referenced by the overlay as `../bases`.

- [ ] **Step 1:** Copy `hdfs-wal-table-topic-perf-test/automq-broker-separated.yaml` into `deploy/automq-broker/kustomize/bases/application.yaml`, changing the FileIO FQN `kafka.automq.table.io.WebHdfsFileIO` → `com.automq.hdfs.table.io.WebHdfsFileIO` in the `printf` catalog line. Everything else verbatim.

- [ ] **Step 2:** Create `deploy/automq-broker/kustomize/bases/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: magnetar-test
resources:
  - application.yaml
```

- [ ] **Step 3:** Commit.

```bash
git add deploy/automq-broker/kustomize/bases
git commit -m "feat(deploy): add automq-broker kustomize base (Application CR)"
```

---

### Task 2: Broker overlay (magnetar-test)

**Files:**
- Create: `deploy/automq-broker/kustomize/magnetar-test/kustomization.yaml`
- Create: `deploy/automq-broker/kustomize/magnetar-test/patch-image.yaml`

**Interfaces:**
- Consumes: `../bases` from Task 1.
- Produces: a buildable overlay whose render overrides the broker container image.

- [ ] **Step 1:** Create `deploy/automq-broker/kustomize/magnetar-test/patch-image.yaml`:

```yaml
- op: replace
  path: /spec/workload/1/manifest/spec/template/spec/containers/0/image
  value: msbingmtcr.azurecr.io/rongyu/automq:test0712sr
```

- [ ] **Step 2:** Create `deploy/automq-broker/kustomize/magnetar-test/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../bases
patches:
  - path: patch-image.yaml
    target:
      group: apis.clusterfleet.io
      version: v1alpha1
      kind: Application
      name: automq-broker-pelian
```

- [ ] **Step 3:** Render to verify the overlay builds and the JSON6902 path resolves (a wrong path makes kustomize error):

```bash
docker run --rm -v "$PWD":/work -w /work registry.k8s.io/kustomize/kustomize:v5.4.3 \
  build deploy/automq-broker/kustomize/magnetar-test
```
Expected: a single `Application` CR printed; `process.roles broker` present; `image: msbingmtcr.azurecr.io/rongyu/automq:test0712sr` present; no error.

- [ ] **Step 4:** Commit.

```bash
git add deploy/automq-broker/kustomize/magnetar-test
git commit -m "feat(deploy): add automq-broker magnetar-test overlay (image patch)"
```

---

### Task 3: Controller base

**Files:**
- Create: `deploy/automq-controller/kustomize/bases/application.yaml`
- Create: `deploy/automq-controller/kustomize/bases/kustomization.yaml`

**Interfaces:**
- Produces: an `Application` CR named `automq-controller-pelian` in namespace `magnetar-test`; base referenced by the overlay as `../bases`.

- [ ] **Step 1:** Copy `hdfs-wal-table-topic-perf-test/automq-controller-separated.yaml` into `deploy/automq-controller/kustomize/bases/application.yaml` verbatim (no Table Topic config here, so no FQN change).

- [ ] **Step 2:** Create `deploy/automq-controller/kustomize/bases/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: magnetar-test
resources:
  - application.yaml
```

- [ ] **Step 3:** Commit.

```bash
git add deploy/automq-controller/kustomize/bases
git commit -m "feat(deploy): add automq-controller kustomize base (Application CR)"
```

---

### Task 4: Controller overlay (magnetar-test)

**Files:**
- Create: `deploy/automq-controller/kustomize/magnetar-test/kustomization.yaml`
- Create: `deploy/automq-controller/kustomize/magnetar-test/patch-image.yaml`

**Interfaces:**
- Consumes: `../bases` from Task 3.

- [ ] **Step 1:** Create `deploy/automq-controller/kustomize/magnetar-test/patch-image.yaml`:

```yaml
- op: replace
  path: /spec/workload/1/manifest/spec/template/spec/containers/0/image
  value: msbingmtcr.azurecr.io/rongyu/automq:test0712sr
```

- [ ] **Step 2:** Create `deploy/automq-controller/kustomize/magnetar-test/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../bases
patches:
  - path: patch-image.yaml
    target:
      group: apis.clusterfleet.io
      version: v1alpha1
      kind: Application
      name: automq-controller-pelian
```

- [ ] **Step 3:** Render to verify:

```bash
docker run --rm -v "$PWD":/work -w /work registry.k8s.io/kustomize/kustomize:v5.4.3 \
  build deploy/automq-controller/kustomize/magnetar-test
```
Expected: single `Application` CR; `process.roles controller` present; controller Service exposes port 9093; `image: ...:test0712sr` present; no error.

- [ ] **Step 4:** Commit.

```bash
git add deploy/automq-controller/kustomize/magnetar-test
git commit -m "feat(deploy): add automq-controller magnetar-test overlay (image patch)"
```

---

## Verification (whole feature)

Both overlays render cleanly and each yields a complete `Application` CR with the
right role, right Service port, and the overlay's image tag:

```bash
docker run --rm -v "$PWD":/work -w /work registry.k8s.io/kustomize/kustomize:v5.4.3 build deploy/automq-broker/kustomize/magnetar-test
docker run --rm -v "$PWD":/work -w /work registry.k8s.io/kustomize/kustomize:v5.4.3 build deploy/automq-controller/kustomize/magnetar-test
```

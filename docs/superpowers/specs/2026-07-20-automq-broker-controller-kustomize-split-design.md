# Split AutoMQ multi-node deploy into broker/controller kustomize services

Date: 2026-07-20

## Goal

The current `deploy/automq-multi.yaml` is a single combined-mode deployment
(every pod is broker AND controller). Restructure it — mirroring the directory
form used in `MtOnAks/services/automq-broker/kustomize/bases` — so that broker
and controller become two **separate** kustomize-managed services with dedicated
roles (`process.roles=broker` vs `process.roles=controller`).

## Confirmed decisions

1. **Keep the Falcon `Application` CR wrapper** (`apis.clusterfleet.io/v1alpha1`).
   We borrow only the *directory structure form* from MtOnAks, NOT its bare
   `StatefulSet`/`Service` + `KAFKA_CONF_*`→properties convention. Our manifests
   stay: Falcon Application CR, HDFS/WebHDFS backend, `start.sh up --flags`,
   `KAFKA_CFG_*` env.
2. **`bases/` + a single overlay** named `magnetar-test` (not the three-tier
   `bases`/`prod`/`<region>` of MtOnAks — we are single-environment).
3. Directories live under **`deploy/<svc>/kustomize/`**.
4. The existing `deploy/automq-multi.yaml` and `deploy/automq-single.yaml`
   are **kept untouched**.
5. `namespace: magnetar-test` is **hardcoded in the base** (kustomize's
   `namespace:` field cannot reach inside an inlined CR's manifest metadata).

## Structural constraint (why base is one file, not two)

MtOnAks splits `bases/` into `service.yaml` + `stateful-set.yaml` because those
are two top-level Kubernetes objects. The Falcon `Application` CR instead
**inlines** the Service and StatefulSet inside `spec.workload[].manifest` of a
single object. Therefore each service's base is a single `application.yaml`
holding both the Service (`workload[0]`) and the StatefulSet (`workload[1]`).
This is the only shape difference from MtOnAks.

## Target directory layout

```
deploy/
  automq-broker/
    kustomize/
      bases/
        kustomization.yaml      # resources: [application.yaml]
        application.yaml        # Application CR: Service(9092) + StatefulSet(process.roles=broker, 3 replicas)
      magnetar-test/
        kustomization.yaml      # resources: [../bases] + patches: [patch-image.yaml]
        patch-image.yaml        # JSON6902: override container image
  automq-controller/
    kustomize/
      bases/
        kustomization.yaml
        application.yaml        # Application CR: Service(9093) + StatefulSet(process.roles=controller, 1 replica)
      magnetar-test/
        kustomization.yaml
        patch-image.yaml
```

## Content source of truth

The split content is taken from the existing flat files, which already encode
the true separated topology:

- `hdfs-wal-table-topic-perf-test/automq-broker-separated.yaml`
- `hdfs-wal-table-topic-perf-test/automq-controller-separated.yaml`

Topology carried over verbatim:

- **Broker**: `process.roles=broker`, `node.id = 100000 + ordinal`, 3 replicas,
  `podManagementPolicy: Parallel`, brokers are NOT voters. Binds
  `PLAINTEXT://<ip>:9092`; probes target port `kafka` (9092). Table Topic /
  Schema Registry config appended to `broker.properties`.
  `controller.quorum.voters` = `200000@automq-controller-pelian-0.<svc>.<ns>.svc.cluster.local:9093`.
- **Controller**: `process.roles=controller`, `node.id = 200000 + ordinal`,
  1 replica, sole voter. Binds `CONTROLLER://<ip>:9093`; probes target port
  `controller` (9093). No Table Topic config. Smaller heap.
- Service objects: broker Service exposes `kafka` (9092) + `metrics` (9090);
  controller Service exposes `controller` (9093) + `metrics` (9090). Both
  headless (`clusterIP: None`, `publishNotReadyAddresses: true`).

### One correction applied during the split

The separated source files use the **old** FileIO FQN
`kafka.automq.table.io.WebHdfsFileIO`. The recent HDFS module rename makes
`com.automq.hdfs.table.io.WebHdfsFileIO` (as already present in
`deploy/automq-multi.yaml`) the current one. The split manifests use the **new**
FQN.

## Overlay mechanism

The single `magnetar-test` overlay overrides only the container **image** (the
same thing MtOnAks' `prod/` overlay does). Because the image lives inside the
inlined CR at
`spec/workload/1/manifest/spec/template/spec/containers/0/image`, kustomize's
built-in `images:` transformer cannot reach it. We use an explicit **JSON6902**
patch instead:

```yaml
# magnetar-test/patch-image.yaml
- op: replace
  path: /spec/workload/1/manifest/spec/template/spec/containers/0/image
  value: msbingmtcr.azurecr.io/rongyu/automq:<tag>
```

`workload[0]` is the Service, `workload[1]` is the StatefulSet.

`namespace` and `replicas` stay in the base (single environment; not overlaid).

## Verification

Render both overlays and confirm each produces a complete Application CR with the
image tag overridden by the overlay:

```
kustomize build deploy/automq-broker/kustomize/magnetar-test
kustomize build deploy/automq-controller/kustomize/magnetar-test
```

Checks:
- Output is a single valid `Application` CR per service.
- `process.roles` is `broker` / `controller` respectively.
- Broker Service exposes 9092; controller Service exposes 9093.
- The container `image` reflects the overlay's tag, not the base's.

## Out of scope

- No change to `deploy/automq-multi.yaml` or `deploy/automq-single.yaml`.
- No migration to bare StatefulSet/Service or `KAFKA_CONF_*` convention.
- No MinIO backend (HDFS/WebHDFS retained).
- No multi-region / `prod` intermediate overlay.

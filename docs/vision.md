# Project Vision

> Build an end-to-end, production-minded **Data & AI Platform** on a single laptop: data flows from an operational database through reliable ingestion and a lakehouse, gets governed and modelled, and finally powers analytics, machine learning, and AI applications.

## At a glance

| | |
|---|---|
| **What** | A local Data & AI platform, built in small verifiable phases. |
| **Why** | Career portfolio + deep engineering learning. |
| **Foundation** | Data Platform Engineering: CDC ingestion, Medallion lakehouse, governance. |
| **Long-term** | Analytics/BI, then ML/MLOps, then AI applications on governed data. |
| **Current focus** | Phase 0: stabilize the repository and verify the existing baseline. |
| **Not the goal** | Running as many tools as possible. Every component must solve a named problem. |

---

## 1. Why this project exists

1. **Career development**: a technically credible portfolio that shows end-to-end data platform skills.
2. **Engineering learning**: understand how modern data platforms are designed, built, operated, and evolved.

Depth of understanding and reliability of the implementation matter more than the number of tools used.

---

## 2. Target architecture

```
 Operational DB ──CDC──▶ Bronze ──▶ Silver ──▶ Gold ──▶ BI · ML · AI
 (PostgreSQL)            └──────── Lakehouse (Iceberg) ────────┘
        ▲                                ▲
        └── Governance & Observability: quality · lineage · metadata · access ──┘
```

### Domains

| Domain | Purpose | Key capabilities |
|---|---|---|
| **A. Sources & Ingestion** | Capture every change from operational systems, reliably. | CDC from PostgreSQL; batch where appropriate; capture of inserts, updates **and deletes**; idempotent, replayable processing (at-least-once delivery); schema evolution; a change simulator that produces realistic source activity. |
| **B. Lakehouse (Medallion)** | Separate raw data, refined data, and business-ready data. | Incremental processing; historical data handling; table maintenance (compaction, snapshot expiry). See layer rules below. |
| **C. Governance & Observability** | Make data trustworthy and traceable. A cross-cutting concern, not a stage. | Data quality tests; reconciliation against the source; metadata and catalog; lineage and impact analysis; ownership and documentation; access control and auditability; pipeline monitoring; data contracts. |
| **D. Analytics & Data Products** | Expose trusted data for analysis. | SQL analytics; business data marts; dashboards; documented, reusable datasets. |
| **E. ML & MLOps** | Support the machine learning lifecycle. | Feature engineering; experiment tracking; training and evaluation; model versioning, deployment, and monitoring. |
| **F. AI Data Applications** | AI built on governed, trusted data. | RAG; natural-language analytics; AI-assisted discovery and data quality. All of it must respect access control. |

### Medallion layer rules

| Layer | Holds | Rules |
|---|---|---|
| **Bronze** | Source data as received, plus ingestion metadata. | No business logic, no joins. Preserve every change. |
| **Silver** | Cleaned, validated, deduplicated data: current state and history. | Business-independent transformations only. |
| **Gold** | Business-oriented models: facts, dimensions, marts. | Built to answer defined business questions. |

---

## 3. Engineering principles

1. **Understand before implementing**: every significant implementation rests on understood concepts.
2. **Correctness before complexity**: data correctness and reliability come before new technology.
3. **Incremental evolution**: build in independently verifiable milestones.
4. **Resource awareness**: respect the laptop's limits (see §7).
5. **Technology with purpose**: adopt a tool only when it solves an identified problem.
6. **Governance by design**: quality, metadata, lineage, and security from the start.
7. **Production-minded, not production-claimed**: apply production practices without pretending this is a production deployment.
8. **Document decisions**: record significant decisions and trade-offs as ADRs in `docs/adr/`.

---

## 4. Roadmap

| Phase | Focus | Expected outcome | Depends on |
|---|---|---|---|
| **0** | Foundation & stabilization | Reproducible local environment; existing pipeline verified end-to-end | — |
| **1** | CDC & ingestion | Every source change (insert, update, delete) reliably lands in Bronze | 0 |
| **2** | Medallion lakehouse | Correct Silver (state + history) and Gold answering business questions | 1 |
| **3** | Governance & observability maturity | End-to-end lineage, data contracts, access control, monitoring | runs alongside 0–2 |
| **4** | Analytics & BI | Trusted dashboards traceable to Gold | 2 |
| **5** | ML & MLOps | One model through the full lifecycle | 2 |
| **6** | AI applications | AI apps on governed data, respecting access control | 2, 3 |

Phases describe long-term direction, not a commitment to build everything now.

### Cross-cutting concerns start early

Governance is introduced from the first phase where it applies. Phase 3 is where it matures, not where it starts.

| Phase | Governance & observability introduced |
|---|---|
| 0 | Reconciliation checks (source vs. lakehouse), pipeline logging |
| 1 | Ingestion metadata on every record; replication lag monitoring |
| 2 | dbt tests, documentation, dbt lineage |
| 3+ | Cross-tool lineage, contracts, access control, dashboards for pipeline health |

---

## 5. Current scope

**Active milestone: Phase 0, Foundation & Stabilization.**

Goal: a verified baseline. The existing PostgreSQL → Bronze → Silver pipeline runs reproducibly from this repository, and its correctness is checked rather than assumed.

Detailed deliverables and acceptance criteria live in the Phase 0 spec under `.scratch/`.

**Next, in order:** CDC ingestion design (Phase 1), then rebuilding Bronze/Silver/Gold on top of it (Phase 2). Technology choices for each are made through repository inspection and recorded as ADRs.

---

## 6. Scope rules

Classify every new idea before acting on it:

| Class | Meaning | What happens |
|---|---|---|
| **Current milestone** | Directly contributes to the active deliverable. | Do it now. |
| **Prerequisite** | Needed to make the current milestone possible or reliable. | Do it now, and note why. |
| **Future opportunity** | Valuable, but belongs to a later phase. | Record it in `.scratch/`; do not build it. |
| **Out of scope** | Does not serve this vision. | Drop it. |

Long-term objectives that are **not** automatic additions to the current milestone:

- Full-scale distributed streaming infrastructure.
- Enterprise data catalog.
- Complete MLOps platform.
- Production-grade AI agent ecosystem.
- Large-scale orchestration or cloud deployment.
- Additional data sources before the first end-to-end flow is reliable.

---

## 7. Local constraints

- **Hardware**: one laptop running Ubuntu on WSL2 (about 10 GB RAM for Docker, no GPU, ample disk). No cloud or VPS budget.
- **Run selectively**: a small core (source DB, object storage, catalog, query engine) runs by default; orchestration, BI, ML, lineage, and AI tooling start on demand.
- **Demonstrate, don't scale**: capabilities like streaming, lineage, MLOps, and AI are shown at small scale with lightweight tools. Production scalability is explained in ADRs, not claimed.

---

## 8. Definition of success

The project succeeds when it demonstrates more than a collection of running services. It shows the ability to:

- Run the whole platform from a clean clone.
- Design and explain an end-to-end data architecture.
- Build reliable ingestion and transformation pipelines.
- Handle inserts, updates, deletes, and history correctly.
- Prove correctness through tests and reconciliation against the source.
- Trace data from source to every downstream consumer.
- Operate and troubleshoot the platform.
- Explain architectural decisions and their trade-offs.
- Show how trusted data supports BI, ML, and AI.

---

## 9. Open questions

| Question | Needed by |
|---|---|
| Which 2–3 business questions should Gold answer? | Phase 2 |
| Which CDC approach fits the laptop and the learning goals? | Phase 1 (ADR) |

# Documentation Index

## Architecture

| Document | Description |
|----------|-------------|
| [Architecture Overview](agent-guides/architecture.md) | Streamhouse Trio design, data flow, component diagram |
| [Storage Layers](agent-guides/storage-layers.md) | HOT/WARM/COLD layer specs, retention, SQL examples |
| [Data Contracts](agent-guides/data-contracts.md) | Validation rules, quarantine flow, schema-on-write |
| [Streamhouse vs Traditional](streamhouse-vs-traditional.md) | Why Streamhouse over Lakehouse/Lambda |
| [Flink Architecture](FLINK_ARCHITECTURE.md) | Flink jobs, checkpointing, exactly-once |
| [Fluss Guide](FLUSS_GUIDE.md) | HOT layer operations, SQL quirks, limitations |

## Chatbot / AI

| Document | Description |
|----------|-------------|
| [Chatbot Architecture](chatbot-architecture.html) | Interactive visual diagram — LangGraph graph, layer routing, SQL generation |
| [Agentic RAG](agent-guides/agentic-rag.md) | LangGraph agent design, Text-to-SQL, self-correction |
| [Chatbot Architecture (text)](agent-guides/chatbot-architecture.md) | Text version of chatbot design |
| [API Documentation](CHATBOT_API_DOCUMENTATION.md) | REST endpoints, request/response schemas |
| [Quick Reference](CHATBOT_QUICK_REFERENCE.md) | Common queries, curl examples, troubleshooting |

## Data & Storage

| Document | Description |
|----------|-------------|
| [Data Contract Architecture](DATA_CONTRACT_ARCHITECTURE.md) | Contract validation, quarantine, lineage |
| [Frame Evidence Storage](VI_FRAME_EVIDENCE_STORAGE.md) | MinIO evidence frames, URL pattern, retrieval |
| [Frame Evidence (agent guide)](agent-guides/frame-evidence-storage.md) | Implementation details |
| [Trino Query Federation](agent-guides/trino-query-federation.md) | Cross-layer SQL with Trino |
| [Trino Quick Reference](agent-guides/trino-quick-reference.md) | Common Trino SQL patterns |

## Operations

| Document | Description |
|----------|-------------|
| [Stop Mechanism](agent-guides/stop-mechanism.md) | Graceful stop for RTSP streaming services |
| [Testing Guide](TESTING_GUIDE.md) | End-to-end test scenarios, curl examples |

---

> For setup instructions see [QUICKSTART.md](../QUICKSTART.md) and [README.md](../README.md).

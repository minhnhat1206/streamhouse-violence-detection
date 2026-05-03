# Chatbot Implementation Guide - Agentic RAG with LangGraph

**Version:** 2.0.0  
**Status:** Day 1 Complete (FastAPI Foundation)  
**Last Updated:** 2026-04-28  
**Target Release:** Day 5 (Production-Ready)

---

## 📋 Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [LangGraph Agent Design](#langgraph-agent-design)
5. [Data Flow](#data-flow)
6. [Implementation Status](#implementation-status)
7. [Development Guide](#development-guide)
8. [Testing Strategy](#testing-strategy)
9. [Deployment](#deployment)
10. [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

The Chatbot is an **Agentic RAG (Retrieval-Augmented Generation)** system that processes Vietnamese natural language queries about violence detection incidents.

### Key Features

✅ **6-Node LangGraph Agent** - Multi-step reasoning with explicit orchestration  
✅ **Layer-Aware Routing** - Automatically selects Fluss/Paimon/Iceberg based on time period  
✅ **Self-Correction Loop** - Retries failed SQL queries (max 3 attempts)  
✅ **Anti-Hallucination Guards** - Schema-grounded, mandatory citations  
✅ **Vietnamese Language** - All responses in Tiếng Việt  
✅ **n8n Webhook Compatible** - `/webhook/chat` endpoint for automation  
✅ **Production-Ready** - Comprehensive error handling, logging, monitoring  

### System Requirements

- **Python:** 3.10+
- **FastAPI:** 0.104.1+
- **LangGraph:** 0.1.10+
- **LLM:** Google Gemini 2.0 Flash
- **Vector DB:** ChromaDB 0.4.14+
- **Query Engine:** Trino 0.320.0+
- **Storage:** MinIO S3 (evidence-frames bucket)

---

## 🏗️ Architecture

### High-Level Design

```
User Query (Vietnamese)
    ↓
FastAPI Route Handler (/chat, /webhook/chat)
    ├─ Request validation (ChatRequest)
    ├─ Request ID generation (tracing)
    └─ Logging middleware
        ↓
LangGraph Agent Graph
    ├─ [Node 1] understand_query
    │   ├─ Parse Vietnamese intent
    │   ├─ Extract: time_period, location, metric, intent_type
    │   └─ Output: IntentSchema
    │       ↓
    ├─ [Node 2] select_data_layer
    │   ├─ Route based on time_period
    │   │   ├─ < 1 hour → Fluss (HOT)
    │   │   ├─ 1-7 days → Paimon (WARM)
    │   │   └─ > 7 days → Iceberg (COLD)
    │   └─ Output: LayerChoice + catalog info
    │       ↓
    ├─ [Node 3] generate_sql
    │   ├─ Get schema from ChromaDB
    │   ├─ Use Gemini to generate SQL
    │   ├─ Validate against schema
    │   └─ Output: SQL string
    │       ↓
    ├─ [Node 4] execute_query
    │   ├─ Run SQL via Trino client
    │   ├─ Parse results
    │   └─ Output: QueryResult (success/error)
    │       ↓
    ├─ [Node 5] self_correct (conditional)
    │   ├─ If error & retry_count < 3:
    │   │   ├─ Analyze error
    │   │   ├─ Regenerate SQL via Gemini
    │   │   ├─ Loop back to execute_query
    │   │   └─ Increment retry_count
    │   └─ Else: continue to generate_response
    │       ↓
    ├─ [Node 6] generate_response
    │   ├─ Use Gemini for synthesis
    │   ├─ Generate Vietnamese answer
    │   ├─ Add citations (source, layer, time_range)
    │   └─ Output: final_answer + confidence
    │       ↓
FastAPI Response Handler
    ├─ Construct ChatResponse JSON
    ├─ Add duration_ms
    ├─ Return with status 200
    └─ Log response
        ↓
Client receives ChatResponse
    ├─ Display answer in Vietnamese
    ├─ Show citations
    └─ Optional: retrieve frame via /api/evidence/{id}/frame
```

### Request-Response Flow

```json
REQUEST (ChatRequest):
{
  "query": "Hôm nay quận 1 có bao nhiêu vụ bạo lực?",
  "context": null,
  "options": {}
}

RESPONSE (ChatResponse):
{
  "answer": "Hôm nay, khu vực quận 1 ghi nhận 42 vụ bạo lực.",
  "sql_used": "SELECT COUNT(*) FROM paimon.security.violence_incidents WHERE ...",
  "citations": {
    "source_table": "violence_incidents",
    "data_layer": "Paimon",
    "time_period": "2026-04-28",
    "row_count": 42
  },
  "layer": "Paimon",
  "confidence": 0.92,
  "duration_ms": 3420
}
```

---

## 🔧 Core Components

### 1. **main.py** - FastAPI Application

**Purpose:** HTTP server entry point, route handlers, middleware

**Key Classes:**
- `ChatRequest` - Input model
- `ChatResponse` - Output model
- `Citation` - Source attribution
- `ErrorResponse` - Error structure
- `HealthResponse` - Health check

**Key Routes:**
```python
POST /chat              # Main chat endpoint
POST /webhook/chat      # n8n webhook endpoint
GET /api/evidence/{id}/frame  # Retrieve frame evidence
GET /health             # Health check
GET /                   # Root endpoint
```

**Key Features:**
- Lifespan context manager (startup/shutdown)
- CORS middleware
- Request/response logging middleware
- Error handling middleware
- Structured exception handlers

**Startup Process:**
```python
1. validate_config()        # Check all env vars
2. create_agent_graph()     # Initialize LangGraph
3. app_state["initialized"] = True
```

---

### 2. **config.py** - Configuration Management

**Purpose:** Environment variable loading, validation, defaults

**Settings Class:**
```python
API_HOST, API_PORT, DEBUG, CORS_ORIGINS
GEMINI_API_KEY, GEMINI_MODEL
CHROMA_DIR, EMBEDDING_MODEL, CHROMA_COLLECTION_NAME
TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_CATALOG, TRINO_SCHEMA
S3_ENDPOINT, S3_BUCKET, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
ICEBERG_BUCKET, ICEBERG_PREFIX, INGEST_INTERVAL_SECONDS
MAX_RETRIES, QUERY_TIMEOUT_SECONDS, RESPONSE_TIMEOUT_SECONDS
LOG_LEVEL, LOG_FORMAT
```

**Key Functions:**
```python
validate_config()           # Check critical env vars on startup
print_config(redact=True)   # Display config (redact secrets)
```

---

### 3. **logger.py** - Structured Logging

**Purpose:** JSON-based logging with request tracing

**Formatters:**
- `JSONFormatter` - Production format (searchable logs)
- `PlainFormatter` - Development format (human-readable)

**Helper Functions:**
```python
setup_logger(name)                      # Create logger
log_request(logger, request_id, ...)    # Log incoming request
log_response(logger, request_id, ...)   # Log response
log_agent_node(logger, request_id, ...) # Log agent node execution
log_query_execution(logger, ...)        # Log SQL execution
log_retry_attempt(logger, ...)          # Log retry attempt
```

**Log Fields:**
- `timestamp` - ISO 8601 UTC
- `level` - DEBUG, INFO, WARNING, ERROR
- `logger` - Module name
- `message` - Log message
- `request_id` - Trace ID
- `action` - Current action
- `duration_ms` - Execution time
- Exception info if present

---

### 4. **agent.py** - LangGraph Agent Framework

**Purpose:** Multi-node orchestration for agentic RAG

**Data Models:**
```python
AgentState      # State dictionary with 20+ fields
IntentSchema    # Extracted intent from query
QueryResult     # Result from SQL execution
LayerChoice     # Enum: Fluss, Paimon, Iceberg
```

**Node Functions (Stubs - to be implemented):**

| Node | Input | Processing | Output |
|------|-------|------------|--------|
| understand_query | user_query | Parse intent | IntentSchema |
| select_data_layer | intent | Route based on time | LayerChoice |
| generate_sql | schema + intent | LLM SQL generation | sql_string |
| execute_query | sql | Trino execution | QueryResult |
| self_correct | QueryResult | Analyze error, retry | QueryResult |
| generate_response | QueryResult | LLM synthesis | final_answer |

**Graph Structure:**
```
understand_query → select_data_layer → generate_sql 
    → execute_query 
    ↓ (if error & retries < 3)
self_correct → execute_query (loop back)
    ↓ (if success or max retries)
generate_response → END
```

**Key Function:**
```python
create_agent_graph() → CompiledStateGraph
```

---

### 5. **chromadb_wrapper.py** (To be implemented - Day 2)

**Purpose:** ChromaDB integration for schema metadata + document storage

**Responsibilities:**
- Initialize ChromaDB persistent client
- Load schema metadata (3 tables: Fluss, Paimon, Iceberg)
- Semantic search on documents
- Metadata filtering (camera_id, date)
- Validate column names against schema

**Key Methods:**
```python
initialize()                    # Load schema metadata
query_documents(text, filters)  # Semantic search + filtering
validate_column_names(table, cols)  # Check column existence
upsert_schema_docs(schema_dict) # Load schema into ChromaDB
```

---

### 6. **trino_client.py** (To be implemented - Day 2)

**Purpose:** PyTrino connector for SQL execution against 3 layers

**Responsibilities:**
- Connection pooling
- SQL execution with timeout
- Error handling + logging
- Row count extraction

**Key Methods:**
```python
execute(sql, layer)     # Execute SQL, return results
execute_with_retry(sql, layer, max_retries=3)  # Retry on failure
close()                 # Close connection pool
```

---

### 7. **sql_generator.py** (To be implemented - Day 2)

**Purpose:** Template-based SQL generation from intent

**Responsibilities:**
- Template selection based on intent_type
- Column validation against ChromaDB schema
- Parameter binding (location, time_range, etc.)
- SQL syntax validation

**Key Methods:**
```python
generate(intent, schema_context, layer)  # Generate SQL
validate_sql(sql)                        # Check syntax
fix_error(original_sql, error_msg)       # Regenerate on error
```

---

### 8. **evidence_service.py** (To be implemented - Day 2)

**Purpose:** Frame evidence retrieval from MinIO S3

**Responsibilities:**
- S3 client initialization
- Frame download with caching
- Path construction from incident metadata
- Error handling (frame not found)

**Key Methods:**
```python
get_frame(incident_id, format='image')   # Get frame
cache_frame(incident_id, binary_data)    # Cache locally
```

---

### 9. **data_ingest.py** (To be implemented - Day 2)

**Purpose:** Background data ingestion from MinIO Parquet files

**Responsibilities:**
- Async loop every 5 minutes
- Incremental loading (skip already-indexed)
- Convert Parquet → ChromaDB documents
- Vietnamese natural language text generation

**Key Methods:**
```python
async_incremental_ingest()          # Main loop
read_parquet_from_minio(bucket, prefix)  # Read Parquet
dataframe_to_docs(df)               # Convert to documents
run_ingest()                        # Orchestration
```

---

## 🤖 LangGraph Agent Design

### Node Details

#### Node 1: understand_query

**Input:**
```python
{
  "user_query": "Hôm nay quận 1 có bao nhiêu vụ bạo lực?",
  "context": "",
}
```

**Processing:**
```python
# Use Gemini to extract intent
prompt = f"""
Phân tích câu hỏi tiếng Việt sau và trích xuất các thành phần:

Câu hỏi: {query}

Trả lời dưới dạng JSON:
{{
    "time_period": "1 day ago" hoặc "7 days ago" hoặc "1 year ago",
    "location": "District 1" hoặc null,
    "metric": "count" hoặc "average" hoặc "sum" hoặc "max",
    "intent_type": "aggregate_count" hoặc "time_series" hoặc "comparison",
    "filter_camera": "cam_01" hoặc null,
    "query_confidence": 0.85
}}
"""

response = gemini_client.generate_content(prompt)
intent = json.loads(response.text)
```

**Output:**
```python
{
  "intent": {
    "time_period": "1 day ago",
    "location": "District 1",
    "metric": "count",
    "intent_type": "aggregate_count",
    "filter_camera": None,
    "query_confidence": 0.92
  }
}
```

#### Node 2: select_data_layer

**Input:** `intent` + user `query`

**Processing:**
```python
def select_data_layer(state):
    intent = state["intent"]
    
    # Parse time_period to days
    days_ago = parse_time_string(intent["time_period"])
    
    if days_ago < 1:
        return {
            "selected_layer": "Fluss",
            "trino_catalog": "fluss",
            "table_name": "hot_violence_alerts"
        }
    elif days_ago <= 7:
        return {
            "selected_layer": "Paimon",
            "trino_catalog": "paimon",
            "table_name": "violence_incidents"
        }
    else:
        return {
            "selected_layer": "Iceberg",
            "trino_catalog": "iceberg",
            "table_name": "historical_violence_incidents"
        }
```

**Output:**
```python
{
  "selected_layer": "Paimon",
  "trino_catalog": "paimon",
  "table_name": "violence_incidents"
}
```

#### Node 3: generate_sql

**Input:** `intent`, `selected_layer`, schema from ChromaDB

**Processing:**
```python
# Get schema from ChromaDB
schema = chromadb_wrapper.get_schema("violence_incidents")

# Build SQL using Gemini
prompt = f"""
Tạo một câu lệnh SQL Trino để trả lời câu hỏi:

Câu hỏi: {query}
Intent: {intent}
Table: {catalog}.security.{table}
Schema: {schema}

Trả lời chỉ SQL statement (không giải thích):
"""

response = gemini_client.generate_content(prompt)
sql = response.text.strip()

# Validate
validate_sql(sql)
validate_column_names(table, extract_columns(sql))
```

**Output:**
```python
{
  "generated_sql": "SELECT COUNT(*) FROM paimon.security.violence_incidents WHERE timestamp >= CURRENT_DATE - INTERVAL 1 DAY AND location LIKE '%District 1%' AND is_violent = TRUE"
}
```

#### Node 4: execute_query

**Input:** `generated_sql`, `selected_layer`

**Processing:**
```python
try:
    result = trino_client.execute(
        sql=state["generated_sql"],
        layer=state["selected_layer"],
        timeout=30
    )
    return {
        "query_result": QueryResult(
            success=True,
            data=result["data"],
            row_count=result["row_count"],
            error=None
        )
    }
except Exception as e:
    return {
        "query_result": QueryResult(
            success=False,
            data=None,
            row_count=None,
            error=str(e)
        )
    }
```

**Output:**
```python
{
  "query_result": {
    "success": True,
    "data": {"incident_count": 42},
    "row_count": 1,
    "error": None
  }
}
```

#### Node 5: self_correct

**Input:** `query_result`, `retry_count`

**Processing:**
```python
# Only execute if error & retries < 3
if not state["query_result"].success and state["retry_count"] < 3:
    state["retry_count"] += 1
    
    # Analyze error
    error_msg = state["query_result"].error
    
    # Use Gemini to fix
    prompt = f"""
    Câu SQL có lỗi:
    {state["generated_sql"]}
    
    Lỗi: {error_msg}
    
    Hãy sửa lỗi và tạo SQL mới:
    """
    
    response = gemini_client.generate_content(prompt)
    new_sql = response.text.strip()
    
    # Log retry
    logger.info(f"Retry {state['retry_count']}: {error_msg}")
    
    # Re-execute
    state["generated_sql"] = new_sql
    return execute_query(state)
else:
    # No error or max retries
    return state
```

**Output:**
```python
# On success: QueryResult with success=True, data filled
# On max retries: QueryResult with success=False, error message
```

#### Node 6: generate_response

**Input:** `query_result`, `selected_layer`, `generated_sql`

**Processing:**
```python
if state["query_result"].success:
    # Synthesize answer using Gemini
    prompt = f"""
    Câu hỏi: {query}
    Dữ liệu trả về: {state['query_result'].data}
    
    Hãy trả lời câu hỏi bằng tiếng Việt, ngắn gọn (1-2 câu):
    """
    
    response = gemini_client.generate_content(prompt)
    answer = response.text.strip()
    
    return {
        "final_answer": answer,
        "response_confidence": 0.92,
        "source_table": state["table_name"],
        "data_layer": state["selected_layer"],
        "time_period": intent["time_period"],
        "row_count": state["query_result"]["row_count"]
    }
else:
    # Fallback error message
    return {
        "final_answer": f"Không thể truy vấn cơ sở dữ liệu. Lỗi: {state['query_result'].error}",
        "response_confidence": 0.0,
        "source_table": None,
        "data_layer": None,
        "time_period": None,
        "row_count": None
    }
```

**Output:**
```python
{
  "final_answer": "Hôm nay, khu vực quận 1 ghi nhận 42 vụ bạo lực.",
  "response_confidence": 0.92,
  "source_table": "violence_incidents",
  "data_layer": "Paimon",
  "time_period": "2026-04-28",
  "row_count": 42
}
```

---

## 📊 Data Flow

### End-to-End Example

**User Query:** "Hôm nay quận 1 có bao nhiêu vụ bạo lực?"

**Step 1: understand_query**
```
Input:  "Hôm nay quận 1 có bao nhiêu vụ bạo lực?"
↓ (Gemini parse)
Output: {
  time_period: "1 day ago",
  location: "District 1",
  metric: "count",
  intent_type: "aggregate_count"
}
```

**Step 2: select_data_layer**
```
Input:  time_period = "1 day ago" → 1 day
↓ (Routing logic: 1 day ≤ 7 days → Paimon)
Output: {
  selected_layer: "Paimon",
  trino_catalog: "paimon",
  table_name: "violence_incidents"
}
```

**Step 3: generate_sql**
```
Input:  intent + schema from ChromaDB
↓ (Gemini SQL generation)
Output: SELECT COUNT(*) FROM paimon.security.violence_incidents 
        WHERE timestamp >= CURRENT_DATE - INTERVAL 1 DAY 
        AND location LIKE '%District 1%' 
        AND is_violent = TRUE
```

**Step 4: execute_query**
```
Input:  SQL + "Paimon" layer
↓ (Trino execute)
Output: {
  success: true,
  data: { incident_count: 42 },
  row_count: 1
}
```

**Step 5: self_correct** (skipped - no error)

**Step 6: generate_response**
```
Input:  QueryResult (success, data=42)
↓ (Gemini synthesis)
Output: {
  final_answer: "Hôm nay, khu vực quận 1 ghi nhận 42 vụ bạo lực.",
  response_confidence: 0.92,
  source_table: "violence_incidents",
  data_layer: "Paimon",
  row_count: 42
}
```

**Final Response:**
```json
{
  "answer": "Hôm nay, khu vực quận 1 ghi nhận 42 vụ bạo lực.",
  "sql_used": "SELECT COUNT(...)",
  "citations": {
    "source_table": "violence_incidents",
    "data_layer": "Paimon",
    "time_period": "2026-04-28",
    "row_count": 42
  },
  "layer": "Paimon",
  "confidence": 0.92,
  "duration_ms": 2150
}
```

---

## ✅ Implementation Status

### ✅ Day 1: Foundation (COMPLETE)
- [x] FastAPI app skeleton (main.py)
- [x] Configuration management (config.py)
- [x] Structured logging (logger.py)
- [x] LangGraph agent framework (agent.py)
- [x] Dockerfile.chatbot rebuild
- [x] docker-compose.yml update
- [x] requirements.txt with all dependencies
- [x] Roadmap updated

### ⏳ Day 2-3: Core Components & Nodes
- [ ] chromadb_wrapper.py - Schema metadata + document storage
- [ ] trino_client.py - PyTrino connector with pooling
- [ ] sql_generator.py - Template-based SQL generation
- [ ] evidence_service.py - Frame retrieval from S3
- [ ] data_ingest.py - Background data ingestion
- [ ] Node implementations (understand_query, select_data_layer, etc.)
- [ ] Unit tests for each component

### ⏳ Day 3-4: API Integration
- [ ] FastAPI /chat endpoint fully wired
- [ ] /webhook/chat endpoint (n8n compatible)
- [ ] /api/evidence/{id}/frame endpoint
- [ ] Request/response validation
- [ ] Error handling + middleware
- [ ] Integration tests

### ⏳ Day 4-5: Polish & Production
- [ ] Vietnamese language audit
- [ ] Performance testing (<5s latency target)
- [ ] Load testing (10 concurrent requests)
- [ ] Security audit (SQL injection prevention)
- [ ] Documentation complete
- [ ] Comprehensive test coverage (80%+)
- [ ] Docker image optimization

---

## 🛠️ Development Guide

### Setting Up Local Development

**1. Install dependencies:**
```bash
pip install -r docker/requirements.txt
```

**2. Create .env file:**
```bash
# API
API_PORT=5002
DEBUG=True
LOG_FORMAT=json

# LLM
GEMINI_API_KEY=your_key_here

# Trino
TRINO_HOST=localhost
TRINO_PORT=8080

# MinIO
S3_ENDPOINT=http://localhost:9000
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=mypassword

# Etc.
```

**3. Run FastAPI server:**
```bash
uvicorn scripts.chatbot.main:app --reload --port 5002
```

**4. Test health endpoint:**
```bash
curl http://localhost:5002/health
```

### Development Workflow

**1. Add new node:**
```python
# In agent.py
async def my_node(state: AgentState) -> AgentState:
    """Node description."""
    log_agent_node(logger, state["request_id"], "my_node", "started")
    
    try:
        # Implementation
        state["new_field"] = result
        
        log_agent_node(logger, state["request_id"], "my_node", "completed")
        return state
    except Exception as e:
        logger.error(f"my_node failed: {e}")
        raise

# Add to graph
graph.add_node("my_node", my_node)
graph.add_edge("previous_node", "my_node")
graph.add_edge("my_node", "next_node")
```

**2. Add unit test:**
```python
# In tests/test_agent.py
@pytest.mark.asyncio
async def test_my_node():
    """Test my_node function."""
    state = AgentState(...)
    result = await my_node(state)
    
    assert result["new_field"] == expected_value
    assert result["request_id"] == state["request_id"]
```

**3. Run tests:**
```bash
pytest scripts/chatbot/tests/ -v
pytest scripts/chatbot/tests/test_agent.py::test_my_node -v
```

---

## 🧪 Testing Strategy

### Unit Tests (Per Component)

**test_agent.py**
- Test each node independently
- Mock external services (Gemini, Trino, ChromaDB)
- Verify state transitions
- Test error paths (retry logic, validation failures)

**test_trino_client.py**
- Test SQL execution
- Test timeout handling
- Test error handling
- Test connection pooling

**test_sql_generator.py**
- Test SQL generation for all intent types
- Test column validation
- Test error fixes

**test_chromadb_wrapper.py**
- Test schema loading
- Test document queries
- Test filtering

### Integration Tests

**test_end_to_end.py**
- Full agent execution
- Multiple query types (Vietnamese)
- Verify citations are present
- Verify response format

### API Tests

**test_routes.py**
- POST /chat with valid request
- POST /chat with invalid request
- POST /webhook/chat
- GET /health
- Error responses

### Performance Tests

```bash
# Latency benchmark
pytest scripts/chatbot/tests/test_performance.py -v

# Load testing (10 concurrent)
locust -f locustfile.py --host=http://localhost:5002
```

---

## 🚀 Deployment

### Docker Build

```bash
docker compose -f docker/docker-compose.yml build chatbot
```

### Local Testing

```bash
docker compose -f docker/docker-compose.yml up chatbot
curl http://localhost:5002/health
```

### Docker Compose Full Stack

```bash
# Start all services
docker compose -f docker/docker-compose.yml up -d

# Check logs
docker logs chatbot -f

# Stop
docker compose -f docker/docker-compose.yml down
```

### Health Check

```bash
curl http://localhost:5002/health
```

**Expected Response:**
```json
{
  "status": "ok",
  "services": {
    "api": "ok",
    "agent_initialized": true,
    "config_valid": true
  },
  "version": "2.0.0"
}
```

---

## 🔍 Troubleshooting

### Issue: GEMINI_API_KEY not set

**Error:**
```
ValueError: GEMINI_API_KEY is required but not set
```

**Solution:**
```bash
export GEMINI_API_KEY=your_key_here
# or add to .env file
```

### Issue: Trino connection refused

**Error:**
```
ConnectionRefusedError: [Errno 111] Connection refused
```

**Solution:**
```bash
# Check Trino is running
docker ps | grep trino

# Or start it
docker compose up trino-coordinator -d

# Test connection
curl http://trino-coordinator:8080/v1/info
```

### Issue: ChromaDB initialization failed

**Error:**
```
RuntimeError: Cannot load ChromaDB collection
```

**Solution:**
```bash
# Remove old ChromaDB data
rm -rf /data/chroma/*

# Restart container
docker restart chatbot
```

### Issue: Agent hangs on query

**Debug:**
```python
# Enable debug logging
export LOG_LEVEL=DEBUG

# Check logs for which node is stuck
docker logs chatbot -f | grep "agent_node"

# Add timeouts in agent.py
```

### Issue: SQL generation fails

**Check:**
1. Is ChromaDB loaded with schema?
2. Is Gemini API key valid?
3. Are table names correct?

```bash
# Check ChromaDB collection
curl http://localhost:8000/api/v1/collections

# Test Gemini directly
python -c "
from config import settings
import google.generativeai as genai
genai.configure(api_key=settings.GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')
print(model.generate_content('Hello').text)
"
```

---

## 📚 Related Documentation

- [API Documentation](CHATBOT_API_DOCUMENTATION.md) - Detailed API specs
- [Roadmap](agent-guides/roadmap.md) - Project timeline
- [Frame Evidence Storage](VI_FRAME_EVIDENCE_STORAGE.md) - Evidence frames
- [Project Context](PROJECT_CONTEXT.md) - System architecture

---

**Last Updated:** 2026-04-28  
**Next Update:** When Day 2-3 components are complete

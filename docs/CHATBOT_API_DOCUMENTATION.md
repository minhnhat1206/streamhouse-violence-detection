# Chatbot API Documentation

**Version:** 2.0.0  
**Base URL:** `http://localhost:5002`  
**Status:** Production-Ready  
**Last Updated:** 2026-04-28

---

## 📋 Table of Contents

1. [Overview](#overview)
2. [Authentication & Security](#authentication--security)
3. [Core Endpoints](#core-endpoints)
4. [Request/Response Formats](#requestresponse-formats)
5. [Error Handling](#error-handling)
6. [Webhooks (n8n Integration)](#webhooks-n8n-integration)
7. [Rate Limiting & Performance](#rate-limiting--performance)
8. [Code Examples](#code-examples)
9. [Integration Patterns](#integration-patterns)
10. [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

The Chatbot API is a RESTful service for Vietnamese natural language queries about violence detection incidents. It exposes **HTTP endpoints** for real-time RAG (Retrieval-Augmented Generation) queries, evidence retrieval, and system health checks.

### API Characteristics

- **Language:** Vietnamese input/output
- **Protocol:** HTTP/1.1 (JSON)
- **Response Format:** JSON with consistent structure
- **Request ID Tracing:** All requests get unique `request_id` for distributed tracing
- **Structured Logging:** All operations logged as JSON (production)
- **Timeout:** 30 seconds per request
- **Max Payload:** 10MB

---

## 🔐 Authentication & Security

### Current Status
**No authentication** - API designed for internal network use only.

### Recommended Production Security
When deploying to production, add one of:

1. **API Key Authentication**
   ```
   Header: Authorization: Bearer <api-key>
   ```

2. **JWT Token**
   ```
   Header: Authorization: Bearer <jwt-token>
   ```

3. **OAuth2 (via n8n)**
   - n8n handles OAuth2 token management
   - Forward Bearer token in webhook requests

### CORS Configuration
```
Allowed Origins: http://localhost:3000 (React dashboard)
Allowed Methods: GET, POST, OPTIONS
Allowed Headers: Content-Type, Authorization, X-Request-ID
```

---

## 🔌 Core Endpoints

### 1. Chat Query (`POST /chat`)

Main endpoint for agentic RAG queries about violence incidents.

#### Request

```http
POST /chat HTTP/1.1
Host: localhost:5002
Content-Type: application/json

{
  "query": "Hôm nay quận 1 có bao nhiêu vụ bạo lực?",
  "context": {
    "user_id": "user-123",
    "session_id": "sess-abc",
    "source": "dashboard"
  }
}
```

#### Request Schema (Pydantic)

```python
class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=1000, description="Vietnamese query")
    context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Metadata: user_id, session_id, source, timestamp"
    )
```

#### Response (Success)

```json
{
  "request_id": "req-1714287123456-a1b2c3",
  "status": "success",
  "query": "Hôm nay quận 1 có bao nhiêu vụ bạo lực?",
  "response": {
    "answer": "Theo dữ liệu từ Fluss (real-time), quận 1 hôm nay có 5 vụ bạo lực được ghi nhận...",
    "confidence": 0.92,
    "citations": [
      {
        "source_table": "violence_incidents",
        "layer": "hot",
        "time_range": "2026-04-28 00:00 - 23:59",
        "row_count": 5
      }
    ],
    "processing_time_ms": 1543,
    "node_trace": {
      "understand_query": {"intent": "count", "location": "quận 1", "time": "today"},
      "select_data_layer": {"layer": "fluss", "reason": "time_period < 1 hour"},
      "generate_sql": {"sql": "SELECT COUNT(*) FROM violence_incidents WHERE location='quận 1' AND DATE(timestamp)=CURRENT_DATE"},
      "execute_query": {"rows_returned": 1, "execution_time_ms": 234},
      "generate_response": {"language": "vi", "synthesis_time_ms": 312}
    }
  },
  "timestamp": "2026-04-28T14:32:03Z"
}
```

#### Response Schema

```python
class Citation(BaseModel):
    source_table: str
    layer: str  # "hot", "warm", "cold"
    time_range: str
    row_count: int

class ChatResponse(BaseModel):
    request_id: str
    status: str  # "success" or "error"
    query: str
    response: Dict[str, Any]  # Contains: answer, confidence, citations, processing_time_ms, node_trace
    timestamp: str  # ISO 8601
```

#### Status Codes

| Code | Meaning | Reason |
|------|---------|--------|
| 200 | Success | Query processed, answer generated |
| 400 | Bad Request | Invalid query (empty, too long) |
| 408 | Timeout | Query took > 30 seconds |
| 500 | Server Error | LLM failure, ChromaDB error, Trino crash |
| 503 | Service Unavailable | Dependency down (Trino, MinIO, ChromaDB) |

---

### 2. Webhook Chat (`POST /webhook/chat`)

**n8n Integration** - Accept webhook requests from n8n workflows.

#### Request

```http
POST /webhook/chat HTTP/1.1
Host: localhost:5002
Content-Type: application/json
X-N8N-Auth: <n8n-signature>

{
  "item": {
    "query": "Lấy dữ liệu bạo lực quận 1 ngày hôm qua",
    "metadata": {
      "workflow_id": "wf-123",
      "execution_id": "exec-456"
    }
  }
}
```

#### Response

Same as `/chat` endpoint.

#### Usage in n8n

1. **n8n Workflow:** Add "Webhook" trigger node
2. **URL:** `http://chatbot:5002/webhook/chat` (internal Docker network)
3. **Method:** POST
4. **Headers:** None (unless auth enabled)
5. **Body:** Map n8n fields to request structure
6. **Parse Response:** Extract `response.answer` field

#### Example n8n Integration

```json
{
  "nodes": [
    {
      "name": "HTTP Request",
      "type": "n8n-nodes-base.httpRequest",
      "parameters": {
        "url": "http://chatbot:5002/webhook/chat",
        "method": "POST",
        "headers": {
          "Content-Type": "application/json"
        },
        "body": {
          "item": {
            "query": "{{ $json.query }}",
            "metadata": {
              "workflow_id": "{{ $workflow.id }}",
              "execution_id": "{{ $execution.id }}"
            }
          }
        }
      }
    }
  ]
}
```

---

### 3. Evidence Frame Retrieval (`GET /api/evidence/{incident_id}/frame`)

Retrieve supporting evidence (video frame) for an incident.

#### Request

```http
GET /api/evidence/incident-5fb8c4ea/frame HTTP/1.1
Host: localhost:5002
```

#### Query Parameters

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `timestamp` | ISO 8601 | No | Auto-detect | Frame timestamp (e.g., `2026-04-28T14:30:00Z`) |
| `camera_id` | string | No | From incident | Camera ID filter |

#### Response (Success)

```http
HTTP/1.1 200 OK
Content-Type: image/jpeg
Content-Length: 145230
X-Frame-Timestamp: 2026-04-28T14:30:15Z
X-Camera-ID: camera-01
X-Incident-ID: incident-5fb8c4ea

[Binary JPEG data]
```

#### Response (Not Found)

```json
{
  "request_id": "req-1714287123457-x9y8z7",
  "status": "error",
  "error": "Evidence not found",
  "details": "No frame available for incident-5fb8c4ea at specified timestamp",
  "timestamp": "2026-04-28T14:32:10Z"
}
```

#### Status Codes

| Code | Meaning |
|------|---------|
| 200 | Frame retrieved successfully |
| 404 | Incident or frame not found |
| 503 | MinIO unavailable |

---

### 4. Health Check (`GET /health`)

Check service readiness and dependency status.

#### Request

```http
GET /health HTTP/1.1
Host: localhost:5002
```

#### Response (Healthy)

```json
{
  "status": "healthy",
  "timestamp": "2026-04-28T14:32:20Z",
  "version": "2.0.0",
  "dependencies": {
    "gemini": "ok",
    "chromadb": "ok",
    "trino": "ok",
    "minio": "ok"
  },
  "uptime_seconds": 3456
}
```

#### Response (Degraded)

```json
{
  "status": "degraded",
  "timestamp": "2026-04-28T14:32:25Z",
  "version": "2.0.0",
  "dependencies": {
    "gemini": "ok",
    "chromadb": "ok",
    "trino": "failed - connection refused",
    "minio": "ok"
  },
  "uptime_seconds": 3461
}
```

#### Status Codes

| Code | Meaning |
|------|---------|
| 200 | Service healthy (all dependencies ok) |
| 200 | Service degraded (some dependencies down, but responding) |
| 503 | Service critical (cannot respond) |

---

## 📝 Request/Response Formats

### Request Structure

All requests to `/chat` and `/webhook/chat` follow this format:

```python
{
  "query": "string (required, 1-1000 chars)",
  "context": {
    "user_id": "string (optional)",
    "session_id": "string (optional)",
    "source": "string (optional) - e.g., 'dashboard', 'mobile', 'webhook'",
    "timestamp": "ISO 8601 (optional)"
  }
}
```

### Response Structure

All successful responses follow this format:

```python
{
  "request_id": "string - unique identifier for this request",
  "status": "success|error",
  "query": "string - echo of input query",
  "response": {
    # Success case:
    "answer": "string - Vietnamese response",
    "confidence": "float 0-1",
    "citations": [
      {
        "source_table": "string",
        "layer": "hot|warm|cold",
        "time_range": "string",
        "row_count": "int"
      }
    ],
    "processing_time_ms": "int",
    "node_trace": {
      "understand_query": {...},
      "select_data_layer": {...},
      "generate_sql": {...},
      "execute_query": {...},
      "generate_response": {...}
    }
    
    # Error case:
    "error": "string - error message",
    "details": "string - detailed error info",
    "retry_count": "int"
  },
  "timestamp": "ISO 8601"
}
```

### Citation Format

Citations provide data provenance:

```python
{
  "source_table": "violence_incidents",  # Table queried
  "layer": "hot|warm|cold",               # Storage layer used
  "time_range": "2026-04-28 00:00 - 23:59", # Data time period
  "row_count": 5                          # Rows returned
}
```

Layers:
- **hot (Fluss):** Real-time, < 100ms latency, 1-2 hour retention
- **warm (Paimon):** Minutes latency, 7-30 day retention
- **cold (Iceberg):** Historical, years retention

---

## ⚠️ Error Handling

### Error Response Format

```json
{
  "request_id": "req-1714287123458-error",
  "status": "error",
  "query": "Hôm nay quận 1 có bao nhiêu vụ bạo lực?",
  "response": {
    "error": "Query Execution Failed",
    "details": "Trino SQL error: Column 'location' not found in table 'violence_incidents'",
    "error_code": "TRINO_SQL_ERROR",
    "retry_count": 2,
    "node_failed_at": "execute_query"
  },
  "timestamp": "2026-04-28T14:32:30Z"
}
```

### Error Codes

| Code | Meaning | HTTP Status | Recoverable |
|------|---------|-------------|-------------|
| `INVALID_QUERY` | Query validation failed | 400 | No |
| `INTENT_PARSE_ERROR` | Gemini failed to parse intent | 500 | Yes (retry) |
| `CHROMADB_ERROR` | Schema lookup failed | 500 | Yes (retry) |
| `SQL_GENERATION_ERROR` | Gemini SQL generation failed | 500 | Yes (retry) |
| `TRINO_CONNECTION_ERROR` | Cannot connect to Trino | 503 | Yes (auto-retry) |
| `TRINO_SQL_ERROR` | SQL syntax or execution error | 500 | Yes (self-correct) |
| `LLM_ERROR` | Gemini API error (quota, rate limit) | 500 | Yes (backoff) |
| `TIMEOUT` | Request exceeded 30s | 408 | Maybe |
| `SERVICE_UNAVAILABLE` | Dependency critical failure | 503 | No |

### Retry Logic (Client Side)

For recoverable errors (HTTP 500 with `retry_count < 3`):

```javascript
async function chatWithRetry(query, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch('http://localhost:5002/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query })
      });
      
      if (response.status === 200) {
        return await response.json();
      }
      
      if ([500, 503].includes(response.status) && attempt < maxRetries) {
        // Exponential backoff
        await new Promise(r => setTimeout(r, Math.pow(2, attempt) * 1000));
        continue;
      }
      
      throw new Error(`HTTP ${response.status}`);
    } catch (err) {
      if (attempt === maxRetries) throw err;
    }
  }
}
```

---

## 🪝 Webhooks (n8n Integration)

### Webhook Flow

```
n8n Workflow
  ↓ (HTTP POST)
Chatbot API /webhook/chat
  ↓
Parse request
  ↓
Route to /chat handler (same logic)
  ↓
Return response to n8n
  ↓
n8n continues workflow
```

### n8n Webhook Configuration

1. **Trigger Node:** "Webhook"
   - Method: POST
   - URL: `http://chatbot:5002/webhook/chat`

2. **Request Body Mapping:**
   ```json
   {
     "item": {
       "query": "{{ $json.userQuery }}",
       "metadata": {
         "workflow_id": "{{ $workflow.id }}",
         "execution_id": "{{ $execution.id }}"
       }
     }
   }
   ```

3. **Response Parsing:**
   - Extract: `$json.response.answer` → Send to Slack
   - Extract: `$json.response.citations` → Log to database
   - Check: `$json.status === 'error'` → Handle errors

### Example: Slack Notification with Query Results

```json
{
  "nodes": [
    {
      "name": "Chatbot Query",
      "type": "httpRequest",
      "parameters": {
        "url": "http://chatbot:5002/webhook/chat",
        "method": "POST",
        "body": {
          "item": {
            "query": "{{ $json.query }}"
          }
        }
      }
    },
    {
      "name": "Send to Slack",
      "type": "slack",
      "parameters": {
        "message": "🔍 Kết quả: {{ $json.response.answer }}\n📊 Độ tin cậy: {{ $json.response.confidence }}"
      }
    }
  ]
}
```

---

## ⚡ Rate Limiting & Performance

### Limits (Recommended)

| Metric | Limit | Notes |
|--------|-------|-------|
| Requests/min/user | 30 | Per user_id |
| Requests/min/IP | 60 | Across all users |
| Query length | 1000 chars | Validated in request |
| Payload size | 10MB | JSON body |
| Response time | 30s timeout | Server-side timeout |
| Concurrent requests | 32 | Uvicorn workers |

### Performance Targets

| Operation | Target | Actual (Typical) |
|-----------|--------|------------------|
| Query parsing (Node 1) | 100ms | 50-80ms |
| Layer selection (Node 2) | 10ms | 5-10ms |
| SQL generation (Node 3) | 800ms | 600-1000ms |
| Query execution (Node 4) | 500ms | 200-2000ms (varies) |
| Self-correction (Node 5) | 500ms/retry | 400-1500ms |
| Response generation (Node 6) | 300ms | 200-500ms |
| **Total latency** | **2.2s** | **1.5-5.5s** |

### Optimization Tips

1. **Batch Queries:** Send multiple queries in one webhook batch
2. **Cache Results:** Implement Redis caching layer for repeated queries
3. **Filter by Time:** Specify time range in query to route to hot storage
4. **Monitor Trino:** Check Trino query performance for SQL bottlenecks
5. **ChromaDB Indexing:** Ensure ChromaDB is properly indexed

---

## 💻 Code Examples

### Python (requests)

```python
import requests
import json

BASE_URL = "http://localhost:5002"

# 1. Chat query
response = requests.post(
    f"{BASE_URL}/chat",
    json={
        "query": "Hôm nay có bao nhiêu vụ bạo lực tại quận 1?",
        "context": {
            "user_id": "user-123",
            "source": "python_client"
        }
    }
)

data = response.json()
print(f"Answer: {data['response']['answer']}")
print(f"Confidence: {data['response']['confidence']}")
print(f"Citations: {data['response']['citations']}")
```

### JavaScript (fetch)

```javascript
const query = "Hôm nay có bao nhiêu vụ bạo lực tại quận 1?";

const response = await fetch('http://localhost:5002/chat', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    query: query,
    context: {
      user_id: 'user-123',
      source: 'react_dashboard'
    }
  })
});

const data = await response.json();
console.log(data.response.answer);
console.log(data.response.citations);
```

### JavaScript (axios)

```javascript
import axios from 'axios';

const chatAPI = axios.create({
  baseURL: 'http://localhost:5002',
  timeout: 30000
});

async function askChatbot(query) {
  try {
    const { data } = await chatAPI.post('/chat', {
      query,
      context: { user_id: 'user-123' }
    });
    return data.response.answer;
  } catch (error) {
    console.error(error.response?.data || error.message);
  }
}

// Usage
const answer = await askChatbot("Lấy tổng số vụ bạo lực tuần này");
```

### cURL

```bash
curl -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Hôm nay có bao nhiêu vụ bạo lực tại quận 1?",
    "context": {
      "user_id": "user-123"
    }
  }'
```

### Health Check (bash)

```bash
# Check service health
curl http://localhost:5002/health

# Check and pretty-print
curl -s http://localhost:5002/health | python3 -m json.tool
```

---

## 🔗 Integration Patterns

### Pattern 1: React Dashboard Integration

```javascript
// services/chatbotAPI.js
export async function queryChatbot(query) {
  const response = await fetch('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      query,
      context: { source: 'dashboard' }
    })
  });
  
  if (!response.ok) {
    throw new Error(`API Error: ${response.status}`);
  }
  
  return response.json();
}

// components/ChatInterface.jsx
import { useState } from 'react';
import { queryChatbot } from '../services/chatbotAPI';

export function ChatInterface() {
  const [answer, setAnswer] = useState('');
  const [loading, setLoading] = useState(false);

  async function handleQuery(query) {
    setLoading(true);
    try {
      const { response } = await queryChatbot(query);
      setAnswer(response.answer);
    } catch (err) {
      setAnswer(`Error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="chat-interface">
      <textarea 
        placeholder="Đặt câu hỏi tiếng Việt..."
        onKeyPress={(e) => e.key === 'Enter' && handleQuery(e.target.value)}
      />
      {loading ? <div>Đang xử lý...</div> : <p>{answer}</p>}
    </div>
  );
}
```

### Pattern 2: n8n Workflow

```json
{
  "nodes": [
    {
      "name": "Trigger",
      "type": "trigger",
      "parameters": {
        "event": "scheduled",
        "interval": 3600
      }
    },
    {
      "name": "Query Chatbot",
      "type": "httpRequest",
      "parameters": {
        "url": "http://chatbot:5002/webhook/chat",
        "method": "POST",
        "body": {
          "item": {
            "query": "Lấy tổng số vụ bạo lực trong 24 giờ qua"
          }
        }
      }
    },
    {
      "name": "Parse Response",
      "type": "function",
      "parameters": {
        "code": "return { answer: $json.response.answer, confidence: $json.response.confidence }"
      }
    },
    {
      "name": "Save to DB",
      "type": "postgres",
      "parameters": {
        "query": "INSERT INTO chatbot_logs (answer, confidence) VALUES ($1, $2)",
        "values": ["{{ $json.answer }}", "{{ $json.confidence }}"]
      }
    }
  ]
}
```

### Pattern 3: Logging & Monitoring

```python
import logging
import requests
from datetime import datetime

logger = logging.getLogger('chatbot_client')

class ChatbotLogger:
    def __init__(self, log_file='chatbot_requests.jsonl'):
        self.log_file = log_file
        self.api_url = 'http://localhost:5002/chat'
    
    def query(self, query, user_id):
        start_time = datetime.utcnow()
        response = requests.post(
            self.api_url,
            json={"query": query, "context": {"user_id": user_id}}
        )
        duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        data = response.json()
        
        # Log to JSONL
        log_entry = {
            "request_id": data['request_id'],
            "user_id": user_id,
            "query": query,
            "status": data['status'],
            "confidence": data['response'].get('confidence'),
            "duration_ms": duration_ms,
            "timestamp": data['timestamp']
        }
        
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
        
        return data
```

---

## 🐛 Troubleshooting

### Issue: "Service Unavailable (503)"

**Symptom:** All API requests return 503

**Causes:**
1. Chatbot container not running
2. Trino is down
3. ChromaDB is inaccessible
4. MinIO is offline

**Solution:**
```bash
# Check service status
docker ps | grep chatbot

# View logs
docker logs chatbot

# Check dependencies
curl http://localhost:8082/ui/ # Trino
curl http://localhost:9001/   # MinIO
```

### Issue: "Query Execution Failed" (500)

**Symptom:** Query processes but SQL fails

**Causes:**
1. Incorrect table name (schema mismatch)
2. Column doesn't exist
3. Invalid date format
4. Trino timeout

**Solution:**
```bash
# Access Trino CLI
docker exec -it trino-coordinator trino

# Check tables
SHOW TABLES IN iceberg.default;
SHOW COLUMNS FROM iceberg.default.violence_incidents;

# Test query manually
SELECT COUNT(*) FROM iceberg.default.violence_incidents;
```

### Issue: Slow Responses (> 5 seconds)

**Symptom:** API responds slowly

**Causes:**
1. Trino query is slow (large table scan)
2. ChromaDB indexing issue
3. Gemini API latency
4. Network latency to Trino

**Solution:**
```bash
# Check Trino query performance
docker exec -it trino-coordinator trino
SELECT query_id, state, queued_time_ms, execution_time_ms FROM system.runtime.tasks;

# Check ChromaDB
curl http://localhost:8000/api/v1/collections # ChromaDB REST API

# Monitor Gemini latency
# Enable DEBUG logging in config.py
```

### Issue: "Confidence too low" or "Hallucinated answer"

**Symptom:** Response seems incorrect or uncertain

**Causes:**
1. Data contracts violated (invalid schema)
2. Gemini hallucinating table names
3. Self-correction max retries exceeded
4. Insufficient data in storage layer

**Solution:**
1. Check `node_trace.generate_sql.sql` in response
2. Verify table exists: `SHOW TABLES`
3. Review ChromaDB schema metadata
4. Check data freshness in Fluss/Paimon/Iceberg

### Issue: Timeout (408)

**Symptom:** Request takes > 30 seconds

**Causes:**
1. Gemini API slow
2. Trino executing complex query
3. Large result set
4. Network congestion

**Solution:**
1. Specify time range in query (routes to hot/warm layers)
2. Add LIMIT clause in SQL
3. Check Trino queue: `SELECT count(*) FROM system.runtime.queued_executions`
4. Consider query caching

---

## 📚 Related Documentation

- [Chatbot Implementation Guide](./CHATBOT_IMPLEMENTATION_GUIDE.md)
- [Agentic RAG Design](./agent-guides/agentic-rag.md)
- [Data Contracts](./agent-guides/data-contracts.md)
- [Storage Layers](./agent-guides/storage-layers.md)
- [Architecture](./agent-guides/architecture.md)

---

## 📞 Support & Contact

- **Issues:** Check logs: `docker logs chatbot`
- **Performance:** Monitor Grafana dashboard at `http://localhost:3001`
- **Questions:** Refer to CLAUDE.md project instructions

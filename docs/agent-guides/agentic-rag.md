# Agentic RAG — Intelligent Query System

## Khái Niệm
Agentic RAG = AI assistant tự sinh SQL để trả lời câu hỏi tự nhiên.
Khác với RAG truyền thống (chỉ lookup), Agentic RAG có thể **tự quyết định**, **tự sửa lỗi**.

## Tech Stack
- **Framework**: LangGraph (multi-agent orchestration)
- **LLM**: Google Gemini 2.0 Flash
- **Vector DB**: ChromaDB (context retrieval)
- **Query Engine**: Trino (federated SQL)
- **Backend**: FastAPI

## Agent Architecture (LangGraph)

```
Agent Graph:
├─ Node 1: understand_query
│  └─ Extract: time_period, location, metric, intent
│
├─ Node 2: select_data_layer
│  └─ If time < 1hr → Fluss (HOT)
│  └─ If 1hr < time < 7 days → Paimon (WARM)
│  └─ If time > 7 days → Iceberg (COLD)
│
├─ Node 3: generate_sql
│  └─ Create SELECT query for selected layer
│  └─ Use schema context from ChromaDB
│
├─ Node 4: execute_query
│  └─ Run on Trino via PyTrino
│
├─ Node 5: self_correct (conditional)
│  └─ If query fails → analyze error → regenerate SQL
│  └─ Max 3 retries
│
└─ Node 6: generate_response
   └─ Natural language answer with citations
   └─ Include: source table, time range, query used
```

## Example Flow

### Query: "Có bao nhiêu vụ bạo lực ở quận 1 hôm qua?"

```
1. understand_query
   → time_period: "yesterday" (1 day ago)
   → location: "district_1" (Quận 1)
   → metric: "count"
   → intent: "aggregate_count"

2. select_data_layer
   → yesterday = 1 day ago < 7 days
   → Decision: Use PAIMON (warm)

3. generate_sql
   SELECT COUNT(*) as total_incidents
   FROM paimon.security.violence_incidents
   WHERE CAST(timestamp AS DATE) = CURRENT_DATE - INTERVAL '1' DAY
     AND camera_id LIKE 'cam_Q1%'
     AND is_deleted = FALSE
     AND is_violent = TRUE

4. execute_query
   → Result: 42

5. generate_response
   → "Hôm qua, khu vực quận 1 ghi nhận 42 vụ bạo lực."
   → Citation: {source: "violence_incidents", period: "2025-01-14", layer: "Paimon"}
```

## Self-Correction Logic

```python
def self_correct(state: AgentState) -> AgentState:
    """Retry with corrected SQL if query fails."""
    error = state["last_error"]
    original_sql = state["generated_sql"]
    retry_count = state.get("retry_count", 0)

    if retry_count >= 3:
        return {"response": f"Không thể truy vấn sau 3 lần thử. Lỗi: {error}"}

    # Ask LLM to fix the SQL based on error message
    corrected_sql = llm.invoke(
        f"SQL query failed with error: {error}\n"
        f"Original SQL: {original_sql}\n"
        f"Fix the SQL query."
    )

    return {
        "generated_sql": corrected_sql,
        "retry_count": retry_count + 1
    }
```

## Schema Context (ChromaDB)

ChromaDB lưu metadata về tables để LLM sinh SQL chính xác:

```python
# Ingest schema vào ChromaDB
schemas = [
    {
        "id": "hot_violence_alerts",
        "document": "Table hot_violence_alerts in Fluss. Columns: camera_id (STRING), risk_score (DOUBLE), event_type (STRING), timestamp (TIMESTAMP). Retention: 1-2 hours. Use for real-time queries.",
        "metadata": {"layer": "hot", "catalog": "fluss"}
    },
    {
        "id": "violence_incidents",
        "document": "Table violence_incidents in Paimon. Columns: incident_id (STRING PK), camera_id (STRING), timestamp (TIMESTAMP), risk_score (DOUBLE), confidence (DOUBLE), is_violent (BOOLEAN), event_type (STRING), is_deleted (BOOLEAN). Retention: 7-30 days.",
        "metadata": {"layer": "warm", "catalog": "paimon"}
    },
    {
        "id": "historical_violence_incidents",
        "document": "Table historical_violence_incidents in Iceberg. Same schema as bronze. Partitioned by year/month/day. Retention: years. Use for historical/trend queries.",
        "metadata": {"layer": "cold", "catalog": "iceberg"}
    }
]
```

## FastAPI Integration

```python
# scripts/chatbot/app.py
@app.post("/api/chat")
async def chat(request: ChatRequest):
    result = await agent_graph.ainvoke({
        "query": request.message,
        "chat_history": request.history
    })
    return ChatResponse(
        answer=result["response"],
        sql_used=result.get("generated_sql"),
        source_layer=result.get("selected_layer"),
        citations=result.get("citations", [])
    )
```

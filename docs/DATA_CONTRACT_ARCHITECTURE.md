# Đặc tả Kiến trúc Data Contract (Hợp đồng Dữ liệu)

> **Dự án**: Hệ thống giám sát an ninh thông minh phát hiện bạo lực thời gian thực
> **Kiến trúc**: Streamhouse (Fluss / Paimon / Iceberg)
> **Cập nhật**: 2026-04-11

---

## 1. Khái Niệm & Vai Trò của Data Contract

Trong kiến trúc hệ thống dữ liệu hiện đại, thay vì kiểm tra chất lượng dữ liệu ở cuối đường ống Data Pipeline (schema-on-read), chúng ta áp dụng kiểm tra nghiêm ngặt ngay khi dữ liệu vừa phát sinh từ Camera/AI, được gọi là **Schema-on-write**.

Quy tắc này được gọi là **Data Contract (Hợp đồng dữ liệu)**. 

**Mục tiêu cốt lõi:**
1. **Bảo vệ Hệ thống lõi (Streamhouse)**: Đảm bảo dữ liệu chảy vào lớp bộ nhớ trong (Hot Storage - Fluss) và dài hạn (Iceberg/Paimon) hoàn toàn SẠCH (Valid), tránh rác làm hỏng ứng dụng AI và phân tích.
2. **Cách ly vi phạm (Quarantine)**: Khi AI trả về bản ghi bất thường (lỗi tọa độ, giá trị độ tin cậy vô lý, v.v.), dữ liệu này lập tức bị chặn và chuyển vào khu vực riêng để kỹ sư hệ thống phân tích nguyên nhân mà không ảnh hưởng tới luồng real-time.
3. **Phát hiện lỗi ở tốc độ cao**: Với Apache Flink, Data Contract được thực thi theo mô hình Streaming (từng bản ghi một) với độ trễ tính bằng mili-giây.

---

## 2. Đặc tả Lược đồ Dữ liệu (Schema)

Thực thể cốt lõi truyền từ Mock AI Agent / Camera về hệ thống thông qua Kafka topic `urban-safety-alerts` có cấu trúc JSON sau:

```json
{
  "camera_id": "cam_03",
  "timestamp": "2026-04-11T12:22:04.182332+00:00",
  "is_violent": false,
  "risk_score": 0.0315,
  "confidence": 0.4969,
  "event_type": null,
  "location": {
    "city": "TP. Hồ Chí Minh",
    "district": "Quận 1",
    "ward": "Phường Bến Thành",
    "street": "Đường Nguyễn Thái Học",
    "lat": 10.77407,
    "long": 106.70229
  },
  "metadata": {
    "fps": 29, 
    "latency_ms": 21, 
    "mock": true
  }
}
```

---

## 3. Quy Tắc Kiểm Tra Chất Lượng (Validation Rules)

Mỗi bản tin đi qua hệ thống Flink sẽ chịu sự kiểm tra của 5 bộ quy tắc bẻ gãy (Rejection Rules) và quy tắc Cảnh báo (Warning Rules):

| Rule ID / Mã lỗi | Ràng buộc (Constraint) | Hành động khi vi phạm |
| :--- | :--- | :--- |
| `FUTURE_TIMESTAMP` | Thời gian gửi `timestamp` không được lớn hơn hiện tại quá 1 phút (Chống trôi thời gian máy chủ). | **REJECT** (Cách ly) |
| `INVALID_TIMESTAMP_FORMAT` | Chuỗi `timestamp` phải đúng chuẩn ISO-8601, nếu không parse được sẽ bị đánh dấu lỗi. | **REJECT** (Cách ly) |
| `INVALID_CAMERA_ID` | `camera_id` phải khớp biểu thức chính quy `^cam_\d{2}$` (Ví dụ: `cam_01`, `cam_12`). | **REJECT** (Cách ly) |
| `RISK_SCORE_OUT_OF_RANGE`| Mức độ rủi ro `risk_score` phải nằm trong giới hạn thực `0.0 <= x <= 1.0`. | **REJECT** (Cách ly) |
| `CONFIDENCE_OUT_OF_RANGE`| Độ tin cậy `confidence` phải nằm trong giới hạn thực `0.0 <= x <= 1.0`. | **REJECT** (Cách ly) |
| `MISSING_EVENT_TYPE` | Nếu `is_violent` = `true` (Xảy ra bạo lực), bắt buộc phải có mô tả nhãn `event_type` đi kèm (FIGHTING, SHOOTING...). | **REJECT** (Cách ly) |
| `LOW_CONFIDENCE_CRITICAL`| Nếu `event_type` thuộc nhóm nguy hiểm cao ('STABBING', 'SHOOTING') thì độ tin cậy `confidence` nên >= 0.85. | **WARNING** (Cảnh báo, không chặn) |

---

## 4. Kiến Trúc Luồng Dữ Liệu (Validation Data Flow)

Hợp đồng dữ liệu chia dòng thông tin thành 2 nhánh hoàn toàn riêng biệt. Cụ thể đoạn đường ống được xử lý bằng Flink DataStream:

1. **[Nguồn phát]** `inference_mock` xả dữ liệu thô vào **Kafka** (Topic: `urban-safety-alerts`).
2. **[Kiểm định]** Flink Source lấy JSON, giải mã (Deserialize) và chạy qua module `data_contract_validator.py` để nhúng 2 thẻ trạng thái:
   * `"violations"`: Mảng chứa các chi tiết mã lỗi.
   * `"is_valid"`: true / false (Dựa trên số lỗi xuất hiện).
3. **[Định tuyến con]**:
   * **Nhánh Xanh (Valid)**: Nếu `is_valid == true`, Flink gửi record chuẩn đã lọc sang Topic: `hot-violence-alerts-valid` (Sắp tới sẽ đấu cấu trúc này thẳng vào **Apache Fluss** / Paimon).
   * **Nhánh Đỏ (Quarantine)**: Nếu `is_valid == false`, Flink bứng riêng record lỗi kèm theo giải nghĩa `violations` vào Topic: `urban-safety-quarantine` để kỹ sư truy xuất nguyên nhân mô hình sai lệch.

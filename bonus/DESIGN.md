# Bonus Design — Pipeline Phát Hiện Gian Lận Giao Dịch Real-Time cho Ngân Hàng Số tại Việt Nam

## Bài Toán & Ràng Buộc Thực

Một ngân hàng số (neobank) tại Việt Nam xử lý trung bình 2 triệu giao dịch mỗi ngày qua app mobile — chuyển khoản nội bộ, thanh toán QR, nạp ví điện tử, và mua trả góp online. Tỷ lệ gian lận khoảng 0.03%, nhưng mỗi giao dịch bị bỏ sót trung bình thiệt hại 8–15 triệu VND và ảnh hưởng đến điểm NPS. Mục tiêu: pipeline gắn nhãn rủi ro cho từng giao dịch trong vòng **500ms** kể từ khi lệnh được khởi tạo — trước khi core banking xử lý settlement.

**Ràng buộc lộn xộn thực tế:**

- **Tốc độ vs độ trễ nhãn:** Gian lận chỉ được xác nhận sau khi khách hàng báo cáo (trung bình 2–5 ngày sau giao dịch) hoặc sau khi đội fraud ops xem xét (T+1 đến T+7). Không thể train model trên nhãn tươi — phải quản lý label window cẩn thận.
- **Class imbalance cực đoan:** 0.03% fraud ~ 600 giao dịch gian lận / ngày trên 2 triệu giao dịch. Model naive predict "hợp lệ" toàn bộ đạt accuracy 99.97% — vô nghĩa hoàn toàn.
- **Feature drift theo mùa:** Hành vi giao dịch thay đổi mạnh dịp Tết, 11/11, Black Friday, mùa khai giảng. Model train vào tháng 8 sẽ bị drift nặng vào tháng 1.
- **Cold start cho tài khoản mới:** Khách mở tài khoản hôm nay, chưa có lịch sử — feature 30 ngày = NULL toàn bộ. Pipeline phải fallback sang heuristic thay vì crash.
- **Circular 02/2023 và quy định NHNN:** Mọi quyết định từ chối giao dịch phải có audit trail đủ để giải trình với Ngân hàng Nhà nước. Black-box model không được phép là lý do duy nhất để block giao dịch.

---

## Kiến Trúc Đề Xuất

```
  Nguồn sự kiện
  ┌─────────────────────────────────────────────────────────┐
  │ App mobile (iOS/Android)  ──► Kafka topic: txn.raw      │
  │ Internet Banking          ──► Kafka topic: txn.raw      │
  │ POS / QR terminal         ──► Kafka topic: txn.raw      │
  │ Partner API (ví, telco)   ──► Kafka topic: txn.raw      │
  └─────────────────────────────────────────────────────────┘
                    │
                    ▼  (< 50ms)
         ┌──────────────────────┐
         │  Stream Enrichment   │  Flink job: join với
         │  & Feature Assembly  │  feature store (Redis),
         │                      │  device fingerprint DB,
         │                      │  velocity counter (sliding
         │                      │  window 1/5/30 phút)
         └──────────┬───────────┘
                    │  enriched event
                    ▼  (< 200ms tổng)
         ┌──────────────────────┐
         │   Scoring Service    │  gRPC call tới model server
         │   (FastAPI / Triton) │  → fraud_score ∈ [0,1]
         │                      │  → rule engine overlay
         └──────────┬───────────┘
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
    score < 0.4          score ≥ 0.4
    → APPROVE            → REVIEW / BLOCK
    (pass-through)       + push notification
                         + alert fraud ops queue
                    │
                    ▼
         ┌──────────────────────┐
         │  Audit Log (Kafka    │  append-only, S3 + Bronze
         │  → S3 Bronze)        │  Parquet, không xóa
         └──────────┬───────────┘
                    │
                    ▼  (nightly batch)
         ┌──────────────────────┐
         │  Label Collection    │  join outcome từ
         │  & Training Dataset  │  fraud_ops_decisions,
         │                      │  customer_disputes
         └──────────┬───────────┘
                    │
                    ▼  (weekly)
         ┌──────────────────────┐
         │  Model Retraining    │  ASOF JOIN, class
         │  & Champion/Challenger│  resampling, shadow deploy
         └──────────────────────┘
```

---

## Câu Hỏi Then Chốt & Quyết Định

### 1. Feature Nào Có Thể Tính Trong 500ms?

**Quyết định:** Tách feature thành hai tầng — **precomputed** (đọc từ Redis, O(1)) và **real-time counters** (Flink sliding window, cập nhật liên tục).

**Precomputed** (tính nightly, lưu Redis với TTL 26 giờ):
- `avg_txn_amount_30d` — giá trị giao dịch trung bình 30 ngày
- `top_merchant_category` — ngành hàng giao dịch nhiều nhất
- `usual_txn_hours` — khung giờ giao dịch thông thường (bitmap 24 bit)
- `device_count_30d` — số thiết bị khác nhau đã dùng

**Real-time counters** (Flink, sliding window):
- `txn_count_1m` / `txn_count_5m` — số giao dịch 1 và 5 phút gần nhất
- `amount_sum_1m` — tổng tiền 1 phút gần nhất
- `new_recipient_flag` — người nhận này đã từng giao dịch chưa (Bloom filter)

**Đánh đổi:** Precomputed lag tối đa 26 giờ — chấp nhận được vì hành vi dài hạn ổn định. Real-time counter tươi hoàn toàn nhưng chỉ capture velocity, không capture profile. Kết hợp hai tầng cho coverage tốt nhất trong ngân sách latency.

---

### 2. Xử Lý Class Imbalance Thế Nào Khi Train?

**Quyết định:** Không oversample ở data layer — thay vào đó dùng `class_weight` trong loss function và calibrate probability output thành điểm rủi ro có ý nghĩa kinh doanh.

Lý do không dùng SMOTE hay random oversampling: fraud pattern rất cụ thể theo ngữ cảnh (thiết bị lạ + giờ khuya + recipient mới + amount cao bất thường). Synthetic sample tạo ra vector không tồn tại trong thực tế, làm model học boundary sai.

Thay vào đó:
1. `class_weight = {0: 1, 1: 3333}` (tỷ lệ nghịch với tần suất)
2. Calibration bằng Platt scaling trên validation set riêng biệt
3. Threshold tuning tối ưu F2-score (ưu tiên recall vì cost of miss >> cost of false positive)

---

### 3. ASOF JOIN Cho Training — Tại Sao Bắt Buộc?

**Quyết định:** Mọi feature join vào training set đều phải dùng ASOF JOIN tại `txn_timestamp`, không được dùng giá trị feature hiện tại.

**Ví dụ rò rỉ dữ liệu:** Giao dịch gian lận xảy ra lúc 23:00 ngày 5/3. Đến ngày 8/3, fraud ops xác nhận và gắn nhãn fraud=1. Nếu pipeline join `avg_txn_amount_30d` vào ngày 8/3 thay vì lúc 23:00 ngày 5/3 — feature đã bao gồm 3 ngày giao dịch bổ sung, profile bị sai.

```sql
-- Đúng: lấy feature tại thời điểm giao dịch
SELECT
    t.txn_id,
    t.txn_timestamp,
    t.amount,
    f.avg_txn_amount_30d,
    f.device_count_30d,
    l.is_fraud
FROM transactions t
ASOF LEFT JOIN feature_history f
    ON t.user_id = f.user_id
    AND t.txn_timestamp >= f.valid_from
LEFT JOIN fraud_labels l
    ON t.txn_id = l.txn_id
WHERE l.confirmed_at < NOW() - INTERVAL '7 days'
```

Unit test: chạy cả ASOF JOIN và naive JOIN, assert rằng `avg_txn_amount_30d` khớp nhau cho ít nhất 95% hàng — nếu lệch nhiều hơn, pipeline có leak.

---

### 4. Label Delay — Chỉ Train Trên Nhãn Đã Xác Nhận

**Quyết định:** Chỉ đưa giao dịch vào training set khi `confirmed_at IS NOT NULL AND confirmed_at < NOW() - 7d`.

Ba nguồn nhãn, độ trễ khác nhau:
- **Customer dispute** (khách báo cáo trong app): T+0 đến T+30, median T+3
- **Fraud ops manual review**: T+1 đến T+7, xử lý queue mỗi ngày
- **Chargeback từ partner**: T+30 đến T+120, chậm nhất nhưng ground truth chắc nhất

Pipeline gộp cả ba nguồn, lấy nhãn "fraud" nếu bất kỳ nguồn nào xác nhận. Giao dịch chưa có nhãn sau 90 ngày được coi là "hợp lệ" (negative label) — đây là assumption cần theo dõi nếu tỷ lệ dispute muộn tăng.

---

### 5. Model Serving & Audit Trail cho Quy Định NHNN

**Quyết định:** Mọi quyết định block phải kèm theo `reason_codes` có thể giải thích, lưu vào audit log bất biến (append-only S3).

Thay vì trả về chỉ score, scoring service trả về:

```json
{
  "txn_id": "TXN-20240315-001",
  "fraud_score": 0.87,
  "decision": "BLOCK",
  "reason_codes": [
    "HIGH_VELOCITY_1MIN",
    "NEW_RECIPIENT",
    "UNUSUAL_HOUR",
    "AMOUNT_3X_ABOVE_AVG"
  ],
  "model_version": "v2.3.1",
  "evaluated_at": "2024-03-15T23:01:02.341Z"
}
```

`reason_codes` được sinh từ SHAP top-k features tại inference time (không phải post-hoc). Mỗi quyết định block được lưu vào bảng `fraud_decisions` có partition theo ngày, không bao giờ bị xóa — đáp ứng yêu cầu lưu trữ 5 năm của Thông tư 02/2023.

---

## Phương Án Bị Loại

**Rule-based engine duy nhất (không có ML):**
Bộ luật cứng (amount > 50M VND → review, giao dịch nước ngoài → block) có recall thấp vì fraudster học cách né. Ngoài ra, rule engine cần cập nhật thủ công mỗi khi pattern mới xuất hiện — lag 2–4 tuần so với khi fraud được phát hiện. Giữ rule engine như một lớp overlay trên top của ML score, không phải thay thế.

**Full real-time feature computation (không có precomputed):**
Tính `avg_txn_amount_30d` real-time yêu cầu aggregate 30 ngày × 2M giao dịch / ngày = ~60M rows mỗi lần scoring. Không thể đạt 500ms SLA. Precomputed feature với TTL 26 giờ là trade-off đúng đắn cho bài toán này.

---

## Yếu Tố Đặc Thù Việt Nam

- **Chuyển khoản 24/7:** VietQR và Napas 247 cho phép chuyển khoản bất kỳ lúc nào — fraud đỉnh điểm lúc 1–4 giờ sáng khi người dùng ngủ và khó phát hiện kịp thời.
- **SIM swap qua đại lý telco:** Một pattern gian lận phổ biến là hoán đổi SIM để chiếm OTP. Feature `sim_age_days` (thời gian SIM đã dùng) và `sim_changed_recently` là signal quan trọng — cần partnership với Viettel/Mobifone.
- **Giao dịch nhỏ thăm dò (probing):** Fraudster thường chuyển 1,000–10,000 VND trước để kiểm tra tài khoản còn hoạt động không. `micro_txn_flag` (amount < 50,000 VND đến recipient mới) là feature có precision cao.
- **VND và số lớn:** Ngưỡng alert phải tính theo VND, không USD. 50,000,000 VND (~2,000 USD) là giao dịch bình thường với nhiều khách VIP nhưng bất thường với tài khoản phổ thông — cần segment khách hàng trước khi set threshold.

Reflection — Day 17 (≤ 200 words)
1. The flywheel. Bước nguy hiểm nhất là flatten() trong traces.py. Nếu đệ quy bị thiếu một nhánh con, Bronze vẫn có dữ liệu nhưng thiếu span — downstream vẫn chạy, eval/DPO pair vẫn build được, chỉ là sai lặng lẽ. Cách phát hiện: so sánh tổng n_spans trả về với tổng span đếm trực tiếp từ file JSON gốc mỗi lần ingest; alert nếu lệch.

2. Decontamination. Nếu bỏ bước này, model train trên đúng những prompt dùng để chấm điểm → eval score bị thổi phồng. Cụ thể: DPO pair có prompt trùng eval set sẽ khiến model "học thuộc" câu trả lời đúng cho eval, nhưng ngoài thực tế thì không tổng quát hóa được. Dấu hiệu: eval loss thấp bất thường trong khi production accuracy không tăng — khoảng cách train/real ngày càng doãng.

3. Point-in-time. Điểm tín dụng (credit score) trong hệ thống phê duyệt vay. Nếu join giá trị score mới nhất thay vì score tại thời điểm nộp đơn, model thấy score đã cập nhật sau khi khoản vay được duyệt — dữ liệu tương lai rò vào training, model học nhầm pattern.

4. Graph vs vector. Graph trả lời tốt câu hỏi đa bước: "Widget giao từ kho nào?" — cần 2 hop (widget → accessory → warehouse Hanoi) mà không có chunk nào chứa đủ cả hai fact này. Vector foil trong kg_demo.py xác nhận điều đó. Ngược lại, graph là overkill cho câu hỏi đơn như "Chính sách đổi trả là gì?" — một chunk duy nhất đã đủ, không cần traverse.
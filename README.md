# Telegram Shop Bot

Bot Telegram bán hàng tự động, hỗ trợ Premium Emoji, nạp tiền qua QR ngân hàng, mua bằng số dư hoặc thanh toán trực tiếp, đa ngôn ngữ (Việt/Anh), auto-clean tin nhắn, hoạt động tốt trong group và private chat.

---

## Tính năng chính

### Người dùng
- Xem sản phẩm — Danh sách sản phẩm từ API shop, có ảnh + mô tả + giá.
- Mua hàng 2 cách:
  - Trả bằng số dư (nạp trước qua QR).
  - Thanh toán trực tiếp — Quét QR đúng số tiền, bot tự giao hàng.
- Mã giảm giá — Nhập coupon khi mua, tự động kiểm tra qua API.
- Nạp tiền tự động — Chuyển khoản đúng nội dung, bot tự cộng tiền (1-2 phút).
- Lịch sử mua hàng — Xem 10 đơn gần nhất, tải file tài khoản.
- Đa ngôn ngữ — Tiếng Việt / English.
- Tài liệu API — Tải file .md hoặc mở link Developer.
- Hỗ trợ — Liên hệ admin.

### Admin
- Thống kê — Users, số dư, đơn hàng, lợi nhuận, đơn QR chờ, chiết khấu.
- Quản lý users — Danh sách, tìm user, khóa/mở khóa.
- Đơn hàng — 20 đơn mới nhất toàn hệ thống.
- Đơn chờ QR — Danh sách đơn thanh toán trực tiếp đang chờ.
- Nạp thủ công — Cộng tiền cho user theo ID.
- Chiết khấu — Cấu hình hoa hồng theo % hoặc số tiền cố định.
- Broadcast — Gửi thông báo tới tất cả user.
- Cấu hình — Xem thông tin hệ thống.

Stack:
- node-telegram-bot-api — Telegram Bot API
- axios — Gọi API shop + API bank
- node-cron — Quét giao dịch mỗi 15s khi có giao dịch
- qrcode — Tạo QR VietQR
- mysql2/promise — MySQL (tùy chọn)
- redis - lock giao dịch trạng thái bla bla lưu cache cho nhẹ khỏi tốn ram
- dotenv — Đọc .env

---

## Cài đặt

### 1. Yêu cầu
- Node.js >= 16
- npm hoặc yarn
- (Tùy chọn) MySQL >= 5.7 nếu dùng DB_MODE=mysql
- node namefile.js --addsql để tiêm bảng dữ liệu vào database

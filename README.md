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
- Nạp tiền tự động — Chuyển khoản đúng nội dung, bot tự cộng tiền (8-15 giây).
- Lịch sử mua hàng — Xem tất cả đơn gần nhất, tải file tài khoản.
- Đa ngôn ngữ — Tiếng Việt / English.
- Tài liệu API — Tải file .md hoặc mở link Developer.
- Hỗ trợ — Liên hệ admin.

### Admin
- Thống kê — Users, số dư, đơn hàng, lợi nhuận, đơn QR chờ, chiết khấu.
- Quản lý users — Danh sách, tìm user, khóa/mở khóa.
- Đơn hàng — tất cả đơn mới nhất toàn hệ thống.
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
- (Tuỳ chọn) pm2 để khời chạy ngầm hiệu quả
- npm hoặc yarn
- (Tùy chọn) MySQL >= 5.7 nếu dùng DB_MODE=mysql
- node namefile.js --addsql để tiêm bảng dữ liệu vào database

An automated sales Telegram bot featuring Premium Emoji support, bank QR top-ups, purchases via balance or direct payment, multi-language support (Vietnamese/English), automatic message cleanup, and seamless operation in both groups and private chats.

---

## Key Features

### Users
- View Products — Product list fetched from the shop API, including images, descriptions, and prices.
- Two Purchase Methods:
- Pay with balance (pre-loaded via QR). 
- Direct payment — Scan QR for the exact amount; the bot delivers the product automatically.
- Discount Codes — Enter coupons during purchase; automatic validation via API.
- Automatic Top-up — Transfer funds with the correct payment reference; the bot automatically credits the account (8-15 second).
- Purchase History — View the all most recent orders and download account files.
- Multi-language — Vietnamese / English.
- API Documentation — Download .md file or open the Developer link.
- Support — Contact the admin.

### Admin
- Statistics — Users, balances, orders, profits, pending QR orders, and discounts.
- User Management — User list, search, and lock/unlock functions.
- Orders — View the all most recent system-wide orders.
- Pending QR Orders — List of pending direct payment orders.
- Manual Top-up — Credit funds to a user via their ID.
- Discounts — Configure commissions based on a percentage or fixed amount.
- Broadcast — Send notifications to all users.
- Configuration — View system information. Stack:
- node-telegram-bot-api — Telegram Bot API
- axios — API calls (shop + bank)
- node-cron — Transaction scanning (every 15s when transactions occur)
- qrcode — VietQR generation
- mysql2/promise — MySQL (optional)
- redis — Transaction locking, state management, and caching to reduce RAM usage
- dotenv — .env file loading

---

## Installation

### 1. Requirements
- Node.js >= 16
-(Optional) using a pm2 to be more efficient when running
- npm or yarn
- (Optional) MySQL >= 5.7 if using `DB_MODE=mysql`
- Run `node namefile.js --addsql` to inject database tables

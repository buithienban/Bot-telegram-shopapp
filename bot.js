require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const cron = require('node-cron');
const QRCode = require('qrcode');
const TelegramBot = require('node-telegram-bot-api');
const Redis = require('ioredis');
const crypto = require('crypto');

const ADMIN_IDS = (process.env.ADMIN_IDS || '')
    .split(',').map((s) => s.trim()).filter(Boolean);

const CFG = {
    BOT_TOKEN: process.env.BOT_TOKEN,
    SHOP_API_BASE: process.env.SHOP_API_BASE,
    SHOP_API_TOKEN: process.env.SHOP_API_TOKEN,
    BANK_API_URL: process.env.BANK_API_URL,
    BANK_ACCOUNT: process.env.BANK_ACCOUNT,
    BANK_NAME: (process.env.BANK_NAME || '').toUpperCase().trim(),
    BANK_HOLDER: process.env.BANK_HOLDER || process.env.BANK_NAME,
    DB_MODE: (process.env.DB_MODE || 'json').toLowerCase(),
    MYSQL: {
        host: process.env.MYSQL_HOST,
        user: process.env.MYSQL_USER,
        password: process.env.MYSQL_PASSWORD,
        database: process.env.MYSQL_DATABASE,
    },
    REDIS: {
        host: process.env.REDIS_HOST || '127.0.0.1',
        port: Number(process.env.REDIS_PORT) || 6379,
        password: process.env.REDIS_PASSWORD || undefined,
        db: Number(process.env.REDIS_DB) || 0,
    },
    REDIS_PREFIX: process.env.REDIS_PREFIX || 'shopbot:',
    REDIS_ENABLED: (process.env.REDIS_ENABLED || 'true').toLowerCase() === 'true',
    SUPPORT_CONTACT: process.env.SUPPORT_CONTACT || '@support',
    PENDING_TTL_MIN: 15,
    USE_PREMIUM_EMOJI: (process.env.USE_PREMIUM_EMOJI || 'true').toLowerCase() === 'true',
    AUTO_CLEAN: (process.env.AUTO_CLEAN || 'true').toLowerCase() === 'true',
    MAX_TRACKED_MSG: 5,
    STATE_TTL_SEC: 600,
    BUY_LOCK_SEC: 15,
    STUCK_ORDER_MIN: 10,
};

const API_INFO = {
    docsUrl: 'https://idapplegiare.com/developer-api',
    baseUrl: CFG.SHOP_API_BASE,
};

const API_DOC_MD = `# Tài Liệu Tích Hợp API

Base URL: \`https://idapplegiare.com\`

Authentication: Header \`Authorization: Bearer <API_TOKEN>\`.

---

## 1. Xem số dư
**Endpoint**: \`GET /api/v1/user/balance\`

## 2. Sản phẩm
**Endpoint**: \`GET /api/v1/products\`

## 3. Mua sản phẩm
**Endpoint**: \`POST /api/v1/purchases\`
Payload: \`{ "productId": "UUID", "quantity": 1, "couponCode": "optional" }\`

## 4. Tra cứu đơn
**Endpoint**: \`GET /api/v1/purchases/:id\`

## 5. Mã lỗi
- **Unauthorized**: Token không hợp lệ
- **Số dư không đủ**: Không đủ tiền
- **Kho không đủ tài khoản**: Hết hàng
- **Mã giảm giá lỗi**: Coupon không hợp lệ
- **Không tìm thấy sản phẩm**: ID không tồn tại

Định dạng: \`{ "success": false, "error": "..." }\`

---

Developer: https://idapplegiare.com/developer-api
`;

const API_DOC_PATH = path.join(__dirname, 'docs', 'developer-api.md');
const API_DOC_FILENAME = 'Tai-Lieu-API-idapplegiare.md';

function ensureApiDoc() {
    if (!fs.existsSync(path.dirname(API_DOC_PATH))) {
        fs.mkdirSync(path.dirname(API_DOC_PATH), { recursive: true });
    }
    if (!fs.existsSync(API_DOC_PATH)) {
        fs.writeFileSync(API_DOC_PATH, API_DOC_MD, 'utf8');
        console.log('[API Doc] Đã tạo file:', API_DOC_PATH);
    }
}

const BANK_BIN = {
    VCB: '970436', VIETCOMBANK: '970436',
    TCB: '970407', TECHCOMBANK: '970407',
    MB: '970422', MBBANK: '970422',
    VTB: '970415', VIETTINBANK: '970415',
    ACB: '970416', VPB: '970432', VPBANK: '970432',
    TPB: '970423', TPBANK: '970423',
    STB: '970403', SACOMBANK: '970403',
    BIDV: '970418', AGRIBANK: '970405',
    VIB: '970441', SHB: '970443',
    EIB: '970431', EXIMBANK: '970431',
    MSB: '970426', OCB: '970448', SCB: '970429',
    CAKE: '546034', TIMO: '963388',
};

const JSON_FILE = path.join(__dirname, 'data', 'users.json');
const LEDGER_FILE = path.join(__dirname, 'data', 'ledger.json');
const ORDER_STATE_FILE = path.join(__dirname, 'data', 'order_state.json');
const SETTINGS_FILE = path.join(__dirname, 'data', 'settings.json');
const isAdmin = (tid) => ADMIN_IDS.includes(String(tid));

const ErrorCodes = {
    VALIDATION_ERROR: 'VALIDATION_ERROR',
    PAYMENT_ERROR: 'PAYMENT_ERROR',
    SHOP_API_ERROR: 'SHOP_API_ERROR',
    DATABASE_ERROR: 'DATABASE_ERROR',
    TELEGRAM_ERROR: 'TELEGRAM_ERROR',
    NETWORK_ERROR: 'NETWORK_ERROR',
    UNKNOWN_ERROR: 'UNKNOWN_ERROR',
};

class AppError extends Error {
    constructor(code, message, meta = {}) {
        super(message);
        this.code = code;
        this.meta = meta;
    }
}

const RedisClient = (() => {
    if (!CFG.REDIS_ENABLED) return null;
    const client = new Redis({
        host: CFG.REDIS.host,
        port: CFG.REDIS.port,
        password: CFG.REDIS.password,
        db: CFG.REDIS.db,
        keyPrefix: CFG.REDIS_PREFIX,
        lazyConnect: false,
        maxRetriesPerRequest: 3,
        enableReadyCheck: true,
    });
    client.on('connect', () => console.log('[Redis] Connecting...'));
    client.on('ready', () => console.log('[Redis] Ready'));
    client.on('error', (e) => console.error('[Redis] Error:', e.message));
    return client;
})();

const R = {
    async get(key) {
        if (!RedisClient) return null;
        try { return await RedisClient.get(key); }
        catch (e) { console.error('[Redis.get]', e.message); return null; }
    },
    async set(key, value, ttlSec = null) {
        if (!RedisClient) return false;
        try {
            if (ttlSec) await RedisClient.set(key, value, 'EX', ttlSec);
            else await RedisClient.set(key, value);
            return true;
        } catch (e) { console.error('[Redis.set]', e.message); return false; }
    },
    async setNX(key, value, ttlSec = null) {
        if (!RedisClient) return false;
        try {
            let res;
            if (ttlSec) res = await RedisClient.set(key, value, 'EX', ttlSec, 'NX');
            else res = await RedisClient.set(key, value, 'NX');
            return res === 'OK';
        } catch (e) { console.error('[Redis.setNX]', e.message); return false; }
    },
    async setJSON(key, obj, ttlSec = null) {
        return this.set(key, JSON.stringify(obj), ttlSec);
    },
    async getJSON(key) {
        const raw = await this.get(key);
        if (!raw) return null;
        try { return JSON.parse(raw); }
        catch { return null; }
    },
    async del(key) {
        if (!RedisClient) return false;
        try { await RedisClient.del(key); return true; }
        catch (e) { console.error('[Redis.del]', e.message); return false; }
    },
    async exists(key) {
        if (!RedisClient) return false;
        try { return (await RedisClient.exists(key)) === 1; }
        catch { return false; }
    },
    async incrBy(key, n) {
        if (!RedisClient) return null;
        try { return await RedisClient.incrby(key, n); }
        catch (e) { console.error('[Redis.incrBy]', e.message); return null; }
    },
    async hset(key, field, value) {
        if (!RedisClient) return false;
        try { await RedisClient.hset(key, field, value); return true; }
        catch (e) { console.error('[Redis.hset]', e.message); return false; }
    },
    async hget(key, field) {
        if (!RedisClient) return null;
        try { return await RedisClient.hget(key, field); }
        catch { return null; }
    },
    async hdel(key, field) {
        if (!RedisClient) return false;
        try { await RedisClient.hdel(key, field); return true; }
        catch { return false; }
    },
    async hgetall(key) {
        if (!RedisClient) return {};
        try { return await RedisClient.hgetall(key); }
        catch { return {}; }
    },
    async keys(pattern) {
        if (!RedisClient) return [];
        try { return await RedisClient.keys(pattern); }
        catch { return []; }
    },
    async expire(key, ttlSec) {
        if (!RedisClient) return false;
        try { await RedisClient.expire(key, ttlSec); return true; }
        catch { return false; }
    },
    async rpush(key, value) {
        if (!RedisClient) return false;
        try { await RedisClient.rpush(key, value); return true; }
        catch { return false; }
    },
    async lrange(key, start, stop) {
        if (!RedisClient) return [];
        try { return await RedisClient.lrange(key, start, stop); }
        catch { return []; }
    },
    async ltrim(key, start, stop) {
        if (!RedisClient) return false;
        try { await RedisClient.ltrim(key, start, stop); return true; }
        catch { return false; }
    },
    async delPattern(pattern) {
        if (!RedisClient) return 0;
        try {
            const found = await RedisClient.keys(pattern);
            if (!found.length) return 0;
            const stripped = found.map(k => k.replace(CFG.REDIS_PREFIX, ''));
            return await RedisClient.del(...stripped);
        } catch (e) { console.error('[Redis.delPattern]', e.message); return 0; }
    },
};

const StateKeys = {
    admin: (uid) => `state:admin:${uid}`,
    coupon: (uid) => `state:coupon:${uid}`,
    tracked: (chatId, uid) => `msg:tracked:${chatId}:${uid}`,
    msgOwner: (msgId) => `msg:owner:${msgId}`,
    buyLock: (uid, pid) => `lock:buy:${uid}:${pid}`,
    depositLock: (ref) => `lock:deposit:${ref}`,
    purchaseLock: (orderId) => `lock:purchase:${orderId}`,
};

const StateStore = {
    async setAdmin(userId, obj) {
        await R.setJSON(StateKeys.admin(userId), obj, CFG.STATE_TTL_SEC);
    },
    async getAdmin(userId) {
        return await R.getJSON(StateKeys.admin(userId));
    },
    async delAdmin(userId) {
        await R.del(StateKeys.admin(userId));
    },
    async setCoupon(userId, obj) {
        await R.setJSON(StateKeys.coupon(userId), obj, CFG.STATE_TTL_SEC);
    },
    async getCoupon(userId) {
        return await R.getJSON(StateKeys.coupon(userId));
    },
    async delCoupon(userId) {
        await R.del(StateKeys.coupon(userId));
    },
    async setMsgOwner(msgId, userId) {
        await R.set(StateKeys.msgOwner(msgId), String(userId), 3600);
    },
    async getMsgOwner(msgId) {
        return await R.get(StateKeys.msgOwner(msgId));
    },
    async pushTracked(chatId, userId, msgId) {
        const key = StateKeys.tracked(chatId, userId);
        await R.rpush(key, String(msgId));
        await R.ltrim(key, -CFG.MAX_TRACKED_MSG, -1);
        await R.expire(key, 3600);
    },
    async getTracked(chatId, userId) {
        return await R.lrange(StateKeys.tracked(chatId, userId), 0, -1);
    },
    async clearTracked(chatId, userId) {
        await R.del(StateKeys.tracked(chatId, userId));
    },
    async acquireBuyLock(userId, productId) {
        const key = StateKeys.buyLock(userId, productId);
        const token = crypto.randomBytes(8).toString('hex');
        const ok = await R.setNX(key, token, CFG.BUY_LOCK_SEC);
        return ok ? token : null;
    },
    async releaseBuyLock(userId, productId, token) {
        const key = StateKeys.buyLock(userId, productId);
        const cur = await R.get(key);
        if (cur === token) await R.del(key);
    },
    async acquireDepositLock(ref) {
        const key = StateKeys.depositLock(ref);
        return await R.setNX(key, '1', 300);
    },
    async releaseDepositLock(ref) {
        await R.del(StateKeys.depositLock(ref));
    },
};

const EMOJI = {
    welcome: { id: '5343939171725617118', unicode: '💫' },
    id_card: { id: '5465262274031659421', unicode: '🪪' },
    rank: { id: '6088920147072915408', unicode: '🏆' },
    balance: { id: '5445353829304387411', unicode: '💎' },
    total_in: { id: '5197434882321567830', unicode: '💸' },
    orders: { id: '6089177836520742034', unicode: '📜' },
    products: { id: '5193065010795911968', unicode: '🛍' },
    deposit: { id: '5409048419211682843', unicode: '💵' },
    language: { id: '5447410659077661506', unicode: '🌐' },
    support: { id: '5465169893580086142', unicode: '☎️' },
    help: { id: '5460641585005864052', unicode: '📖' },
    admin: { id: '5296369303661067030', unicode: '🔒' },
    loading: { id: '6298691319086712919', unicode: '😄' },
    refresh: { id: '6298608963088812117', unicode: '☺️' },
    success: { id: '6296367896398399651', unicode: '✅' },
    fail: { id: '6298671811345254603', unicode: '😭' },
    info: { id: '6255793039705377676', unicode: '🔺' },
    warn: { id: '6255512604110751681', unicode: '🔻' },
    time: { id: '6255572871091849620', unicode: '✔️' },
    ok: { id: '6053134758735513473', unicode: '🆗' },
    order2: { id: '6325800285075671264', unicode: '🙂' },
    speed: { id: '5224607267797606837', unicode: '☄️' },
    vip: { id: '6219549292458150316', unicode: '👑' },
    rank2: { id: '6244241334320762892', unicode: '💎' },
    chat: { id: '6181322172263308706', unicode: '💬' },
    pin: { id: '5397782960512444700', unicode: '📌' },
    broadcast: { id: '5424818078833715060', unicode: '📣' },
    mail: { id: '5253742260054409879', unicode: '✉️' },
    plus: { id: '5397916757333654639', unicode: '➕' },
    back: { id: '5852777596688797905', unicode: '⬅️' },
    search: { id: '5355252098403412825', unicode: '🔍' },
};

let PREMIUM_OK = CFG.USE_PREMIUM_EMOJI;

function P(key) {
    const e = EMOJI[key] || EMOJI.plus;
    if (!e) return '';
    if (!PREMIUM_OK) return '';
    return `<tg-emoji emoji-id="${e.id}">${e.unicode}</tg-emoji>`;
}

function U(key) {
    const e = EMOJI[key];
    return e ? e.unicode : '';
}

function CE(key) {
    const e = EMOJI[key];
    if (!e || !PREMIUM_OK) return {};
    return { icon_custom_emoji_id: e.id };
}

function stripPremiumEmoji(str) {
    return String(str ?? '').replace(/<tg-emoji[^>]*>(.*?)<\/tg-emoji>/gs, '$1');
}

const Settings = {
    _default() { return { discount: { type: 'none', value: 0 } }; },
    _read() {
        if (!fs.existsSync(SETTINGS_FILE)) {
            fs.mkdirSync(path.dirname(SETTINGS_FILE), { recursive: true });
            fs.writeFileSync(SETTINGS_FILE, JSON.stringify(this._default(), null, 2));
        }
        try { return JSON.parse(fs.readFileSync(SETTINGS_FILE, 'utf8')); }
        catch { return this._default(); }
    },
    _write(d) { fs.writeFileSync(SETTINGS_FILE, JSON.stringify(d, null, 2)); },
    get() { return this._read(); },
    setDiscount(type, value) {
        const d = this._read();
        d.discount = { type, value: Number(value) || 0 };
        this._write(d);
        return d.discount;
    },
};

function calcPrice(basePrice) {
    const base = Math.round(Number(basePrice) || 0);
    const d = Settings.get().discount || { type: 'none', value: 0 };
    let discount = 0;
    if (d.type === 'percent' && d.value > 0) discount = Math.round(base * d.value / 100);
    else if (d.type === 'fixed' && d.value > 0) discount = Math.round(d.value);
    return {
        base, discount,
        final: Math.max(base + discount, 0),
        discountType: d.type, discountValue: d.value,
    };
}

function discountLabel() {
    const d = Settings.get().discount || { type: 'none', value: 0 };
    if (d.type === 'percent' && d.value > 0) return `+${d.value}%`;
    if (d.type === 'fixed' && d.value > 0) return `+${Number(d.value).toLocaleString('vi-VN')}đ`;
    return 'Không';
}

const I18N = {
    vi: {
        welcome: (n) =>
            `${P('welcome')} Xin chào <b>${n}</b>!\n\n` +
            `Chào mừng đến với <b>Shop tài nguyên</b>.\n\n` +
            `${P('help')} Gõ /help để xem hướng dẫn sử dụng.\n` +
            `${P('products')} Gõ /products để xem sản phẩm.\n` +
            `${P('deposit')} Gõ /deposit để nạp tiền.`,
        welcome_admin: () => `\n\n${P('admin')} <b>Admin</b>: gõ /admin để mở bảng điều khiển.`,

        m_products: 'Sản phẩm',
        m_account: 'Tài khoản',
        m_deposit: 'Nạp tiền',
        m_history: 'Lịch sử',
        m_language: 'Ngôn ngữ',
        m_support: 'Hỗ trợ',
        m_help: 'Help',
        m_admin: 'Admin Panel',
        m_search: 'Tìm kiếm',

        search_prompt: `${P('search')} <b>Tìm kiếm sản phẩm</b>\n\nNhập từ khoá cần tìm:`,
        search_empty: (q) => `${P('fail')} Không tìm thấy sản phẩm nào khớp với <b>${escapeHtml(q)}</b>.`,
        search_result: (q, n) => `${P('search')} <b>Kết quả tìm kiếm: “${escapeHtml(q)}”</b> (${n} sản phẩm)`,

        choose_lang: `${P('language')} Chọn ngôn ngữ:`,
        lang_saved: `${P('success')} Đã đổi sang Tiếng Việt`,
        loading: `${P('loading')} Đang xử lý...`,
        buy_processing: `${P('loading')} Đang xử lý đơn hàng... Vui lòng chờ.`,

        account: (b, id, rank, totalIn) =>
            `${P('id_card')} <b>Tài khoản</b>\n\n` +
            `${P('plus')} ID: <code>${id}</code>\n` +
            `${P('rank')} Cấp bậc: <b>${rank}</b>\n` +
            `${P('balance')} Số dư: <b>${b}</b>\n` +
            `${P('total_in')} Tổng đã nạp: <b>${totalIn}</b>`,

        deposit: (acc, name, code, holder) =>
            `${P('deposit')} <b>NẠP TIỀN TỰ ĐỘNG</b>\n\n` +
            `${P('plus')} Ngân hàng: <b>${name}</b>\n` +
            `${P('id_card')} Chủ TK: <b>${holder}</b>\n` +
            `${P('balance')} Số TK: <code>${acc}</code>\n` +
            `${P('pin')} Nội dung: <code>${code}</code>\n\n` +
            `${P('warn')} <b>Chuyển ĐÚNG nội dung</b> để được cộng tiền tự động (1-2 phút).\n` +
            `${P('info')} Quét QR bên dưới để điền sẵn thông tin.`,

        no_products: `${P('fail')} Không có sản phẩm.`,
        product_list: `${P('products')} <b>Danh sách sản phẩm</b>\nChọn để xem:`,
        product_not_found: `${P('fail')} Không tìm thấy sản phẩm.`,

        price: 'Giá',
        buy_now: 'Mua ngay',

        buy_confirm: (n, p) => `Xác nhận mua <b>${n}</b>\n${P('balance')} Giá: <b>${p}</b>?`,
        confirm: 'Xác nhận',
        cancel: 'Hủy',

        coupon_prompt:
            `${P('balance')} <b>Nhập mã giảm giá</b>\n\n` +
            `Gửi mã giảm giá (nếu có) hoặc bấm <b>Bỏ qua</b>.\n` +
            `VD: <code>SALE10</code>`,
        coupon_skip: 'Bỏ qua',
        coupon_applied: (code) => `${P('success')} Đã áp dụng mã: <code>${code}</code>`,
        coupon_invalid: (e) => `${P('fail')} Mã không hợp lệ: ${e}`,
        coupon_try_again: `${P('pin')} Nhập mã khác hoặc bấm Bỏ qua:`,

        buy_success: (u, p) =>
            `${P('ok')} <b>Mua thành công!</b>\n\n` +
            (p ? `${P('id_card')} Tài khoản: <code>${u}</code>\n${P('plus')} Mật khẩu: <code>${p}</code>`
                : `${P('id_card')} Chi tiết sản phẩm: <code>${u}</code>`),
        buy_success_group: () => `${P('ok')} <b>Mua thành công!</b>\nTài khoản đã được gửi vào tin nhắn riêng của bạn để bảo mật.`,
        buy_fail: (e) => `${P('fail')} Lỗi: ${e}`,

        not_enough_choose: (price, bal) =>
            `${P('warn')} Số dư không đủ.\n\n` +
            `${P('balance')} Giá SP: <b>${price}</b>\n` +
            `${P('total_in')} Số dư: <b>${bal}</b>\n\nChọn cách thanh toán:`,
        pay_with_balance: 'Trả bằng số dư',
        pay_direct: 'Thanh toán trực tiếp',

        pay_direct_title: (name, price) =>
            `${P('speed')} <b>THANH TOÁN TRỰC TIẾP</b>\n\n` +
            `${P('order2')} Sản phẩm: <b>${name}</b>\n` +
            `${P('balance')} Số tiền: <b>${price}</b>\n\n` +
            `Quét QR bên dưới để thanh toán đúng số tiền.\n` +
            `${P('warn')} <b>KHÔNG sửa số tiền và nội dung.</b>\n` +
            `${P('time')} Sau khi chuyển khoản, chờ 1-2 phút, bot sẽ tự giao hàng.`,
        pay_direct_caption: (code, holder, bankName, amount) =>
            `${P('pin')} Mã đơn: <code>${code}</code>\n` +
            `${P('id_card')} Chủ TK: <b>${holder}</b>\n` +
            `${P('plus')} Ngân hàng: <b>${bankName}</b>\n` +
            `${P('balance')} Số tiền: <b>${amount}</b>\n` +
            `${P('info')} Quét bằng app ngân hàng`,
        pay_direct_waiting: `${P('time')} Đang chờ thanh toán...\nĐơn sẽ tự động xử lý khi nhận đủ tiền.`,
        pay_direct_success: (u, p) =>
            `${P('ok')} <b>Thanh toán thành công!</b>\n\n` +
            `${P('id_card')} User: <code>${u}</code>\n` +
            `${P('plus')} Pass: <code>${p}</code>`,
        pay_direct_fail: (e) => `${P('fail')} Không thể hoàn tất đơn: ${e}`,
        pay_direct_expired: `${P('time')} Đơn thanh toán đã hết hạn. Vui lòng tạo lại.`,

        history_empty: `${P('fail')} Chưa có đơn hàng.`,
        history_title: `${P('orders')} <b>Lịch sử mua hàng</b> (10 gần nhất)`,

        support: (c) => `${P('chat')} Hỗ trợ: ${c}`,
        deposit_success: (a) => `${P('ok')} Nạp thành công <b>${a}</b>!`,
        deposit_checking: `${P('loading')} Đang kiểm tra giao dịch...\nVui lòng chờ tối đa 2 phút.`,
        deposit_refreshed: (c) => `${P('refresh')} Đã tạo mã nạp mới: <code>${c}</code>`,
        banned: `${P('fail')} Tài khoản của bạn đã bị khóa.`,
        back: 'Quay lại',

        btn_refresh: `${U('refresh')} Làm mới mã`,
        btn_check: 'Đã chuyển khoản',
        btn_buy_no_code: 'Mua không mã',
        btn_docs: 'Tài liệu API',
        btn_download_docs: 'Tải tài liệu (.md)',
    },
    en: {
        welcome: (n) =>
            `${P('welcome')} Hello <b>${n}</b>!\n\n` +
            `Welcome to <b>Shop Bot</b>.\n\n` +
            `${P('help')} Type /help for instructions.\n` +
            `${P('products')} Type /products to view products.\n` +
            `${P('deposit')} Type /deposit to deposit.`,
        welcome_admin: () => `\n\n${P('admin')} <b>Admin</b>: type /admin to open panel.`,

        m_products: 'Products',
        m_account: 'Account',
        m_deposit: 'Deposit',
        m_history: 'History',
        m_language: 'Language',
        m_support: 'Support',
        m_help: 'Help',
        m_admin: 'Admin Panel',
        m_search: 'Search',

        search_prompt: `${P('search')} <b>Search products</b>\n\nEnter search keyword:`,
        search_empty: (q) => `${P('fail')} No products found matching <b>${escapeHtml(q)}</b>.`,
        search_result: (q, n) => `${P('search')} <b>Search: “${escapeHtml(q)}”</b> (${n} results)`,

        choose_lang: `${P('language')} Choose language:`,
        lang_saved: `${P('success')} Language changed to English`,
        loading: `${P('loading')} Processing...`,
        buy_processing: `${P('loading')} Processing order... Please wait.`,

        account: (b, id, rank, totalIn) =>
            `${P('id_card')} <b>Account</b>\n\n` +
            `${P('plus')} ID: <code>${id}</code>\n` +
            `${P('rank')} Rank: <b>${rank}</b>\n` +
            `${P('balance')} Balance: <b>${b}</b>\n` +
            `${P('total_in')} Total deposited: <b>${totalIn}</b>`,

        deposit: (acc, name, code, holder) =>
            `${P('deposit')} <b>AUTO DEPOSIT</b>\n\n` +
            `${P('plus')} Bank: <b>${name}</b>\n` +
            `${P('id_card')} Holder: <b>${holder}</b>\n` +
            `${P('balance')} Account: <code>${acc}</code>\n` +
            `${P('pin')} Content: <code>${code}</code>\n\n` +
            `${P('warn')} Transfer with <b>EXACT content</b> for auto-credit.\n` +
            `${P('info')} Scan QR below to auto-fill.`,

        no_products: `${P('fail')} No products.`,
        product_list: `${P('products')} <b>Products</b>\nSelect to view:`,
        product_not_found: `${P('fail')} Product not found.`,

        price: 'Price',
        buy_now: 'Buy now',

        buy_confirm: (n, p) => `Buy <b>${n}</b>\n${P('balance')} Price: <b>${p}</b>?`,
        confirm: 'Confirm',
        cancel: 'Cancel',

        coupon_prompt:
            `${P('balance')} <b>Enter coupon code</b>\n\n` +
            `Send a coupon code (if any) or press <b>Skip</b>.\n` +
            `E.g: <code>SALE10</code>`,
        coupon_skip: 'Skip',
        coupon_applied: (code) => `${P('success')} Coupon applied: <code>${code}</code>`,
        coupon_invalid: (e) => `${P('fail')} Invalid coupon: ${e}`,
        coupon_try_again: `${P('pin')} Enter another code or press Skip:`,

        buy_success: (u, p) =>
            `${P('ok')} <b>Purchased!</b>\n\n` +
            (p ? `${P('id_card')} User: <code>${u}</code>\n${P('plus')} Pass: <code>${p}</code>`
                : `${P('id_card')} Product Details: <code>${u}</code>`),
        buy_success_group: () => `${P('ok')} <b>Purchased!</b>\nAccount details have been sent to your private messages for security.`,
        buy_fail: (e) => `${P('fail')} Error: ${e}`,

        not_enough_choose: (price, bal) =>
            `${P('warn')} Insufficient balance.\n\n` +
            `${P('balance')} Price: <b>${price}</b>\n` +
            `${P('total_in')} Balance: <b>${bal}</b>\n\nChoose payment method:`,
        pay_with_balance: 'Pay with balance',
        pay_direct: 'Direct payment',

        pay_direct_title: (name, price) =>
            `${P('speed')} <b>DIRECT PAYMENT</b>\n\n` +
            `${P('order2')} Product: <b>${name}</b>\n` +
            `${P('balance')} Amount: <b>${price}</b>\n\n` +
            `Scan QR below and pay exact amount.\n` +
            `${P('warn')} <b>DO NOT modify amount/content.</b>\n` +
            `${P('time')} After transfer, wait 1-2 minutes.`,
        pay_direct_caption: (code, holder, bankName, amount) =>
            `${P('pin')} Order: <code>${code}</code>\n` +
            `${P('id_card')} Holder: <b>${holder}</b>\n` +
            `${P('plus')} Bank: <b>${bankName}</b>\n` +
            `${P('balance')} Amount: <b>${amount}</b>`,
        pay_direct_waiting: `${P('time')} Waiting for payment...`,
        pay_direct_success: (u, p) =>
            `${P('ok')} <b>Payment successful!</b>\n\n` +
            `${P('id_card')} User: <code>${u}</code>\n` +
            `${P('plus')} Pass: <code>${p}</code>`,
        pay_direct_fail: (e) => `${P('fail')} Cannot complete order: ${e}`,
        pay_direct_expired: `${P('time')} Payment order expired. Please create a new one.`,

        history_empty: `${P('fail')} No orders yet.`,
        history_title: `${P('orders')} <b>Purchase history</b> (last 10)`,

        support: (c) => `${P('chat')} Support: ${c}`,
        deposit_success: (a) => `${P('ok')} Deposited <b>${a}</b>!`,
        deposit_checking: `${P('loading')} Checking transaction...`,
        deposit_refreshed: (c) => `${P('refresh')} New deposit code: <code>${c}</code>`,
        banned: `${P('fail')} Your account has been banned.`,
        back: 'Back',

        btn_refresh: `${U('refresh')} Refresh code`,
        btn_check: 'I have transferred',
        btn_buy_no_code: 'Buy without code',
        btn_docs: 'API Docs',
        btn_download_docs: 'Download Docs (.md)',
    },
};

const t = (lang, key, ...args) => {
    const v = (I18N[lang] || I18N.vi)[key] ?? I18N.vi[key];
    return typeof v === 'function' ? v(...args) : v;
};

function escapeHtml(str) {
    return String(str ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function sanitizeHtml(str) {
    if (!str) return '';
    let out = String(str);
    out = out
        .replace(/<\/?(h[1-6]|div|p|section|article|header|footer|ul|ol|li|tr|table|tbody|thead|blockquote|figure|figcaption)[^>]*>/gi, '\n')
        .replace(/<br\s*\/?>/gi, '\n');
    out = out.replace(/<(?!\/?(b|i|u|s|code|pre|a|tg-emoji)\b)[^>]+>/gi, '');
    out = out.replace(/<a\s+([^>]*)>/gi, (m, attrs) => {
        const href = attrs.match(/href\s*=\s*["']([^"']+)["']/i);
        return href ? `<a href="${href[1]}">` : '<a>';
    });
    out = out.replace(/[ \t]+\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
    return out;
}

const mutexChains = new Map();

function withLock(key, fn) {
    const prev = mutexChains.get(key) || Promise.resolve();
    const next = prev.then(fn, fn);
    mutexChains.set(key, next.catch(() => {}));
    return next;
}

const Ledger = {
    _read() {
        if (!fs.existsSync(LEDGER_FILE)) {
            fs.mkdirSync(path.dirname(LEDGER_FILE), { recursive: true });
            fs.writeFileSync(LEDGER_FILE, JSON.stringify([], null, 2));
        }
        try { return JSON.parse(fs.readFileSync(LEDGER_FILE, 'utf8')); }
        catch { return []; }
    },
    _write(d) { fs.writeFileSync(LEDGER_FILE, JSON.stringify(d, null, 2)); },
    async append(entry) {
        return withLock('ledger', async () => {
            const db = this._read();
            db.push({ ...entry, at: new Date().toISOString() });
            if (db.length > 100000) db.splice(0, db.length - 100000);
            this._write(db);
        });
    },
    async exists(reference, type) {
        return withLock('ledger', async () => {
            const db = this._read();
            return db.some(e => e.reference === reference && e.type === type);
        });
    },
};

const OrderState = {
    _read() {
        if (!fs.existsSync(ORDER_STATE_FILE)) {
            fs.mkdirSync(path.dirname(ORDER_STATE_FILE), { recursive: true });
            fs.writeFileSync(ORDER_STATE_FILE, JSON.stringify({}, null, 2));
        }
        try { return JSON.parse(fs.readFileSync(ORDER_STATE_FILE, 'utf8')); }
        catch { return {}; }
    },
    _write(d) { fs.writeFileSync(ORDER_STATE_FILE, JSON.stringify(d, null, 2)); },
    genId() { return 'ORD' + Date.now().toString(36).toUpperCase() + crypto.randomBytes(3).toString('hex').toUpperCase(); },
    async create(orderId, data) {
        return withLock('order_state', async () => {
            const db = this._read();
            db[orderId] = {
                orderId,
                status: 'CREATED',
                attempts: 0,
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
                ...data,
            };
            this._write(db);
            return db[orderId];
        });
    },
    async get(orderId) {
        return withLock('order_state', async () => this._read()[orderId] || null);
    },
    async update(orderId, patch) {
        return withLock('order_state', async () => {
            const db = this._read();
            if (!db[orderId]) return null;
            db[orderId] = { ...db[orderId], ...patch, updatedAt: new Date().toISOString() };
            this._write(db);
            return db[orderId];
        });
    },
    async findStuck(minutes) {
        return withLock('order_state', async () => {
            const db = this._read();
            const cutoff = Date.now() - minutes * 60 * 1000;
            return Object.values(db).filter(o =>
                ['PAYMENT_PENDING', 'PAYMENT_CONFIRMED', 'PURCHASING', 'DELIVERY_PENDING'].includes(o.status) &&
                new Date(o.updatedAt).getTime() < cutoff
            );
        });
    },
    async recent(limit = 20) {
        return withLock('order_state', async () => {
            const db = this._read();
            return Object.values(db)
                .sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''))
                .slice(0, limit);
        });
    },
};

const DB = (() => {
    const jsonAdapter = {
        _read() {
            if (!fs.existsSync(JSON_FILE)) {
                fs.mkdirSync(path.dirname(JSON_FILE), { recursive: true });
                fs.writeFileSync(JSON_FILE, JSON.stringify({ users: {}, deposits: {} }, null, 2));
            }
            return JSON.parse(fs.readFileSync(JSON_FILE, 'utf8'));
        },
        _write(d) { fs.writeFileSync(JSON_FILE, JSON.stringify(d, null, 2)); },
        async init() { this._read(); console.log('[DB] JSON ready'); },

        async getOrCreateUser(tid, name, username) {
            return withLock('json:db', async () => {
                const db = this._read();
                const id = String(tid);
                if (!db.users[id]) {
                    db.users[id] = {
                        telegramId: id, name: name || '', username: username || '',
                        lang: 'vi', balance: 0, totalIn: 0, profit: 0,
                        depositCode: null, orders: [], banned: false,
                        createdAt: new Date().toISOString(),
                    };
                    this._write(db);
                } else {
                    let ch = false;
                    if (name && db.users[id].name !== name) { db.users[id].name = name; ch = true; }
                    if (username && db.users[id].username !== username) { db.users[id].username = username; ch = true; }
                    if (ch) this._write(db);
                }
                return db.users[id];
            });
        },
        async getUser(tid) {
            return withLock('json:db', async () => {
                return this._read().users[String(tid)] || null;
            });
        },
        async updateUser(tid, patch) {
            return withLock('json:db', async () => {
                const db = this._read(); const id = String(tid);
                if (!db.users[id]) return null;
                db.users[id] = { ...db.users[id], ...patch };
                this._write(db); return db.users[id];
            });
        },
        async addBalanceAtomic(tid, amount, type, reference, extra = {}) {
            return withLock('json:db', async () => {
                const db = this._read();
                const id = String(tid);
                if (!db.users[id]) return null;

                const dup = db.ledger?.some?.(e => e.reference === reference && e.type === type);
                if (dup) return { duplicated: true, user: db.users[id] };

                const oldBalance = db.users[id].balance || 0;
                const newBalance = oldBalance + Math.round(amount);
                db.users[id].balance = newBalance;

                db.ledger = db.ledger || [];
                db.ledger.push({
                    telegramId: id,
                    amount: Math.round(amount),
                    balanceAfter: newBalance,
                    type,
                    reference,
                    metadata: extra,
                    at: new Date().toISOString(),
                });

                this._write(db);
                return { user: db.users[id], balance: newBalance };
            });
        },
        async addBalance(tid, amt, alsoCountTopup = false) {
            return withLock('json:db', async () => {
                const db = this._read(); const id = String(tid);
                if (!db.users[id]) return null;
                const delta = Math.round(amt);
                db.users[id].balance = (db.users[id].balance || 0) + delta;
                if (alsoCountTopup) db.users[id].totalIn = (db.users[id].totalIn || 0) + delta;
                this._write(db); return db.users[id];
            });
        },
        async addTotalIn(tid, amt) {
            return withLock('json:db', async () => {
                const db = this._read(); const id = String(tid);
                if (!db.users[id]) return null;
                db.users[id].totalIn = (db.users[id].totalIn || 0) + Math.round(amt);
                this._write(db); return db.users[id];
            });
        },
        async subBalance(tid, amt) {
            return withLock('json:db', async () => {
                const db = this._read(); const id = String(tid);
                if (!db.users[id] || db.users[id].balance < amt) return null;
                db.users[id].balance -= Math.round(amt);
                this._write(db); return db.users[id];
            });
        },
        async addOrder(tid, order) {
            return withLock('json:db', async () => {
                const db = this._read(); const id = String(tid);
                if (!db.users[id]) return null;
                db.users[id].orders = db.users[id].orders || [];
                const rec = { ...order, createdAt: new Date().toISOString() };
                db.users[id].orders.unshift(rec);
                if (order.profit) {
                    db.users[id].profit = (db.users[id].profit || 0) + Math.round(order.profit);
                }
                this._write(db); return rec;
            });
        },
        async getOrders(tid) {
            return withLock('json:db', async () => {
                return this._read().users[String(tid)]?.orders || [];
            });
        },
        async isDepositProcessed(ref) {
            return withLock('json:db', async () => {
                return !!this._read().deposits[ref];
            });
        },
        async markDepositProcessed(ref, info) {
            return withLock('json:db', async () => {
                const db = this._read();
                if (db.deposits[ref]) return { duplicated: true };
                db.deposits[ref] = { ...info, at: new Date().toISOString() };
                this._write(db);
                return { ok: true };
            });
        },
        async findUserByDepositCode(code) {
            return withLock('json:db', async () => {
                return Object.values(this._read().users).find((u) => u.depositCode === code) || null;
            });
        },
        async setDepositCode(tid, code) { return this.updateUser(tid, { depositCode: code }); },
        async listUsers(limit = 20) {
            return withLock('json:db', async () => {
                return Object.values(this._read().users)
                    .sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''))
                    .slice(0, limit);
            });
        },
        async countUsers() {
            return withLock('json:db', async () => {
                const users = Object.values(this._read().users);
                const balanceSum = users.reduce((s, u) => s + (u.balance || 0), 0);
                const orders = users.reduce((s, u) => s + (u.orders?.length || 0), 0);
                const profit = users.reduce((s, u) => s + (u.profit || 0), 0);
                return { total: users.length, balanceSum, orders, profit };
            });
        },
        async listAllOrders(limit = 20) {
            return withLock('json:db', async () => {
                const db = this._read();
                const out = [];
                for (const u of Object.values(db.users)) {
                    (u.orders || []).forEach((o) =>
                        out.push({ ...o, telegramId: u.telegramId, userName: u.name })
                    );
                }
                return out
                    .sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''))
                    .slice(0, limit);
            });
        },
    };

    const mysql = require('mysql2/promise');
    let pool;
    const mapUser = (r) => r && ({
        telegramId: r.telegram_id, name: r.name, username: r.username,
        lang: r.lang, balance: Number(r.balance),
        totalIn: Number(r.total_in || 0),
        profit: Number(r.profit || 0),
        depositCode: r.deposit_code,
        banned: !!r.banned, createdAt: r.created_at,
    });
    const mysqlAdapter = {
        async init() {
            pool = mysql.createPool({ ...CFG.MYSQL, waitForConnections: true, connectionLimit: 10 });
            console.log('[DB] MySQL connected');

            if (process.argv.includes('--addsql')) {
                await pool.query(`
                    CREATE TABLE IF NOT EXISTS users (
                        telegram_id VARCHAR(50) PRIMARY KEY,
                        name VARCHAR(255),
                        username VARCHAR(255),
                        lang VARCHAR(10) DEFAULT 'vi',
                        balance BIGINT DEFAULT 0,
                        total_in BIGINT DEFAULT 0,
                        profit BIGINT DEFAULT 0,
                        deposit_code VARCHAR(50),
                        banned BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_deposit_code (deposit_code)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                `);
                await pool.query(`
                    CREATE TABLE IF NOT EXISTS orders (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        telegram_id VARCHAR(50) NOT NULL,
                        order_id VARCHAR(100) NOT NULL,
                        product_name VARCHAR(255),
                        price BIGINT DEFAULT 0,
                        base_price BIGINT DEFAULT 0,
                        discount BIGINT DEFAULT 0,
                        quantity INT DEFAULT 1,
                        username TEXT,
                        password TEXT,
                        status VARCHAR(50),
                        profit BIGINT DEFAULT 0,
                        coupon_code VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uk_order_id (order_id),
                        INDEX idx_user_time (telegram_id, created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                `);
                await pool.query(`
                    CREATE TABLE IF NOT EXISTS deposits (
                        ref VARCHAR(100) PRIMARY KEY,
                        amount BIGINT DEFAULT 0,
                        description TEXT,
                        telegram_id VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_user (telegram_id),
                        INDEX idx_created (created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                `);
                await pool.query(`
                    CREATE TABLE IF NOT EXISTS wallet_ledger (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        telegram_id VARCHAR(50) NOT NULL,
                        amount BIGINT NOT NULL,
                        balance_after BIGINT NOT NULL,
                        type VARCHAR(30) NOT NULL,
                        reference VARCHAR(150) NOT NULL,
                        metadata JSON,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uk_ref_type (reference, type),
                        INDEX idx_user_time (telegram_id, created_at),
                        INDEX idx_type_time (type, created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                `);
                await pool.query(`
                    CREATE TABLE IF NOT EXISTS order_state (
                        order_id VARCHAR(100) PRIMARY KEY,
                        telegram_id VARCHAR(50) NOT NULL,
                        product_id VARCHAR(100),
                        product_name VARCHAR(255),
                        status VARCHAR(30) NOT NULL DEFAULT 'CREATED',
                        quantity INT DEFAULT 1,
                        price BIGINT DEFAULT 0,
                        base_price BIGINT DEFAULT 0,
                        profit BIGINT DEFAULT 0,
                        attempts INT DEFAULT 0,
                        last_error TEXT,
                        snapshot JSON,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_status_time (status, updated_at),
                        INDEX idx_user (telegram_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                `);
                console.log('[DB] MySQL tables created/verified via --addsql');
                process.exit(0);
            }
        },
        async getOrCreateUser(tid, name, username) {
            const id = String(tid);
            const [r] = await pool.query('SELECT * FROM users WHERE telegram_id=?', [id]);
            if (!r.length) {
                await pool.query(
                    'INSERT INTO users (telegram_id,name,username,lang,balance,total_in,profit) VALUES (?,?,?,?,0,0,0)',
                    [id, name || '', username || '', 'vi']
                );
            } else {
                await pool.query('UPDATE users SET name=?,username=? WHERE telegram_id=?',
                    [name || r[0].name, username || r[0].username, id]);
            }
            return this.getUser(id);
        },
        async getUser(tid) {
            const [r] = await pool.query('SELECT * FROM users WHERE telegram_id=?', [String(tid)]);
            return mapUser(r[0]);
        },
        async updateUser(tid, patch) {
            const fields = Object.keys(patch)
                .map((k) => `${k.replace(/[A-Z]/g, (c) => '_' + c.toLowerCase())}=?`).join(',');
            const vals = [...Object.values(patch), String(tid)];
            await pool.query(`UPDATE users SET ${fields} WHERE telegram_id=?`, vals);
            return this.getUser(tid);
        },
        async addBalanceAtomic(tid, amount, type, reference, extra = {}) {
            const id = String(tid);
            const delta = Math.round(amount);
            const conn = await pool.getConnection();
            try {
                await conn.beginTransaction();

                const [ins] = await conn.query(
                    `INSERT IGNORE INTO wallet_ledger (telegram_id, amount, balance_after, type, reference, metadata)
                     VALUES (?, ?, 0, ?, ?, ?)`,
                    [id, delta, type, reference, JSON.stringify(extra || {})]
                );

                if (ins.affectedRows === 0) {
                    await conn.rollback();
                    return { duplicated: true };
                }

                await conn.query(
                    `UPDATE users SET balance = balance + ? WHERE telegram_id = ?`,
                    [delta, id]
                );

                const [[u]] = await conn.query(
                    `SELECT balance FROM users WHERE telegram_id = ?`, [id]
                );

                await conn.query(
                    `UPDATE wallet_ledger SET balance_after = ? WHERE reference = ? AND type = ?`,
                    [u.balance, reference, type]
                );

                await conn.commit();
                return { balance: Number(u.balance) };
            } catch (e) {
                await conn.rollback();
                throw e;
            } finally {
                conn.release();
            }
        },
        async addBalance(tid, amt, alsoCountTopup = false) {
            if (alsoCountTopup) {
                await pool.query('UPDATE users SET balance=balance+?, total_in=total_in+? WHERE telegram_id=?', [Math.round(amt), Math.round(amt), String(tid)]);
            } else {
                await pool.query('UPDATE users SET balance=balance+? WHERE telegram_id=?', [Math.round(amt), String(tid)]);
            }
            return this.getUser(tid);
        },
        async addTotalIn(tid, amt) {
            await pool.query('UPDATE users SET total_in=total_in+? WHERE telegram_id=?', [Math.round(amt), String(tid)]);
            return this.getUser(tid);
        },
        async subBalance(tid, amt) {
            const [r] = await pool.query(
                'UPDATE users SET balance=balance-? WHERE telegram_id=? AND balance>=?',
                [Math.round(amt), String(tid), Math.round(amt)]
            );
            return r.affectedRows ? this.getUser(tid) : null;
        },
        async addOrder(tid, o) {
            await pool.query(
                `INSERT INTO orders (telegram_id,order_id,product_name,price,base_price,discount,quantity,username,password,status,profit,coupon_code)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
                [String(tid), o.orderId, o.productName, Math.round(o.price || 0), Math.round(o.basePrice || 0),
                 Math.round(o.discount || 0), o.quantity || 1, o.username, o.password,
                 o.status || 'success', Math.round(o.profit || 0), o.couponCode || null]
            );
            if (o.profit) {
                await pool.query('UPDATE users SET profit=profit+? WHERE telegram_id=?', [Math.round(o.profit), String(tid)]);
            }
            return o;
        },
        async getOrders(tid) {
            const [r] = await pool.query(
                'SELECT * FROM orders WHERE telegram_id=? ORDER BY id DESC LIMIT 10', [String(tid)]
            );
            return r.map((x) => ({
                orderId: x.order_id, productName: x.product_name, price: Number(x.price),
                quantity: x.quantity, username: x.username, password: x.password,
                status: x.status, profit: Number(x.profit), createdAt: x.created_at,
            }));
        },
        async isDepositProcessed(ref) {
            const [r] = await pool.query('SELECT 1 FROM deposits WHERE ref=?', [ref]);
            return r.length > 0;
        },
        async markDepositProcessed(ref, info) {
            const [r] = await pool.query(
                'INSERT IGNORE INTO deposits (ref,amount,description,telegram_id) VALUES (?,?,?,?)',
                [ref, Math.round(info.amount || 0), info.description, info.telegramId || null]
            );
            return r.affectedRows ? { ok: true } : { duplicated: true };
        },
        async findUserByDepositCode(code) {
            const [r] = await pool.query('SELECT * FROM users WHERE deposit_code=?', [code]);
            return mapUser(r[0]);
        },
        async setDepositCode(tid, code) {
            await pool.query('UPDATE users SET deposit_code=? WHERE telegram_id=?', [code, String(tid)]);
            return this.getUser(tid);
        },
        async listUsers(limit = 20) {
            const [r] = await pool.query('SELECT * FROM users ORDER BY created_at DESC LIMIT ?', [limit]);
            return r.map(mapUser);
        },
        async countUsers() {
            const [[u]] = await pool.query('SELECT COUNT(*) total, COALESCE(SUM(balance),0) balanceSum, COALESCE(SUM(profit),0) profit FROM users');
            const [[o]] = await pool.query('SELECT COUNT(*) orders FROM orders');
            return { total: u.total, balanceSum: Number(u.balanceSum), orders: o.orders, profit: Number(u.profit) };
        },
        async listAllOrders(limit = 20) {
            const [r] = await pool.query(
                `SELECT o.*, u.name AS userName FROM orders o
                 LEFT JOIN users u ON u.telegram_id = o.telegram_id
                 ORDER BY o.id DESC LIMIT ?`, [limit]
            );
            return r.map((x) => ({
                telegramId: x.telegram_id, userName: x.userName,
                orderId: x.order_id, productName: x.product_name, price: Number(x.price),
                quantity: x.quantity, username: x.username, status: x.status,
                profit: Number(x.profit), createdAt: x.created_at,
            }));
        },
    };

    return CFG.DB_MODE === 'mysql' ? mysqlAdapter : jsonAdapter;
})();

const Pending = {
    _key: (code) => `pending:${code}`,
    _codePrefix: 'pending:code:',

    async create({ code, telegramId, productId, productName, amount, basePrice, profit, quantity = 1, couponCode = null }) {
        const data = {
            code, telegramId: String(telegramId), productId, productName,
            amount: Math.round(amount), basePrice: Math.round(basePrice || 0),
            profit: Math.round(profit || 0), quantity, couponCode,
            status: 'pending', createdAt: new Date().toISOString(),
        };
        await R.setJSON(this._key(code), data, CFG.PENDING_TTL_MIN * 60 + 3600);
        await R.set(`${this._codePrefix}${code}`, code, CFG.PENDING_TTL_MIN * 60 + 3600);
        return data;
    },
    async get(code) {
        return await R.getJSON(this._key(code));
    },
    async update(code, patch) {
        const cur = await R.getJSON(this._key(code));
        if (!cur) return null;
        const next = { ...cur, ...patch };
        await R.setJSON(this._key(code), next, CFG.PENDING_TTL_MIN * 60 + 3600);
        return next;
    },
    async all() {
        const keys = await R.keys(`${this._codePrefix}*`);
        const out = [];
        for (const k of keys) {
            const code = k.replace(CFG.REDIS_PREFIX + this._codePrefix, '');
            const data = await R.getJSON(this._key(code));
            if (data) out.push(data);
        }
        return out;
    },
    async hasPending() {
        const keys = await R.keys(`${this._codePrefix}*`);
        for (const k of keys) {
            const code = k.replace(CFG.REDIS_PREFIX + this._codePrefix, '');
            const data = await R.getJSON(this._key(code));
            if (data && data.status === 'pending') return true;
        }
        return false;
    },
    async cleanExpired(ttlMin = CFG.PENDING_TTL_MIN) {
        const keys = await R.keys(`${this._codePrefix}*`);
        const now = Date.now();
        for (const k of keys) {
            const code = k.replace(CFG.REDIS_PREFIX + this._codePrefix, '');
            const data = await R.getJSON(this._key(code));
            if (!data || data.status !== 'pending') continue;
            const age = (now - new Date(data.createdAt).getTime()) / 60000;
            if (age > ttlMin) {
                await this.update(code, { status: 'expired' });
            }
        }
    },
};

const ShopAPI = {
    client: axios.create({
        baseURL: CFG.SHOP_API_BASE,
        headers: { Authorization: `Bearer ${CFG.SHOP_API_TOKEN}`, Accept: 'application/json' },
        timeout: 20000,
    }),
    _cache: new Map(),
    _cacheTTL: 30000,
    _cacheGet(key) {
        const c = this._cache.get(key);
        if (!c) return null;
        if (Date.now() > c.exp) { this._cache.delete(key); return null; }
        return c.val;
    },
    _cacheSet(key, val, ttl = this._cacheTTL) {
        this._cache.set(key, { val, exp: Date.now() + ttl });
    },
    async getProducts() {
        const cached = this._cacheGet('products');
        if (cached) return cached;
        try {
            const res = (await this.client.get('/api/v1/products')).data.products || [];
            this._cacheSet('products', res);
            return res;
        } catch (e) {
            if (e.response) throw new AppError(ErrorCodes.SHOP_API_ERROR, e.response?.data?.error || 'Shop API error');
            throw new AppError(ErrorCodes.NETWORK_ERROR, e.message);
        }
    },
    async getCategories() {
        const cached = this._cacheGet('categories');
        if (cached) return cached;
        try {
            const res = (await this.client.get('/api/app/products/categories')).data.categories || [];
            this._cacheSet('categories', res, 60000);
            return res;
        } catch (e) {
            return [];
        }
    },
    async getCoupons() {
        const cached = this._cacheGet('coupons');
        if (cached) return cached;
        try {
            const res = (await this.client.get('/api/app/coupons')).data.coupons || [];
            this._cacheSet('coupons', res, 60000);
            return res;
        } catch (e) {
            return [];
        }
    },
    async validateAndCalcCoupon(productId, basePrice, couponCode) {
        if (!couponCode) return { valid: false, discountAmount: 0 };
        const coupons = await this.getCoupons();
        const c = coupons.find(x => x.code.toUpperCase() === couponCode.toUpperCase());

        if (!c) return { valid: false, error: 'Mã không tồn tại' };
        if (!c.isActive) return { valid: false, error: 'Mã không hoạt động' };

        const now = new Date();
        if (c.startDate && new Date(c.startDate) > now) return { valid: false, error: 'Mã chưa đến thời gian bắt đầu' };
        if (c.endDate && new Date(c.endDate) < now) return { valid: false, error: 'Mã đã hết hạn' };

        if (c.usageLimit !== null && c.usedCount >= c.usageLimit) return { valid: false, error: 'Mã đã hết lượt sử dụng' };

        if (!c.isAllProducts && c.productIds && !c.productIds.includes(productId)) {
            return { valid: false, error: 'Mã không áp dụng cho sản phẩm này' };
        }

        if (c.minOrderAmount > 0 && basePrice < c.minOrderAmount) {
            return { valid: false, error: `Đơn hàng tối thiểu ${Number(c.minOrderAmount).toLocaleString('vi-VN')}đ` };
        }

        let discountAmount = 0;
        if (c.discountType === 'percentage') {
            discountAmount = Math.round(basePrice * (c.discountValue / 100));
        } else if (c.discountType === 'fixed') {
            discountAmount = c.discountValue;
        }

        return { valid: true, discountAmount, coupon: c };
    },
    async purchase(productId, quantity = 1, couponCode = null) {
        const body = { productId, quantity };
        if (couponCode) body.couponCode = couponCode;
        try {
            return (await this.client.post('/api/v1/purchases', body)).data;
        } catch (e) {
            if (e.code === 'ECONNABORTED' || e.code === 'ETIMEDOUT') {
                throw new AppError(ErrorCodes.SHOP_API_ERROR, 'PURCHASE_TIMEOUT');
            }
            if (e.response) {
                return { success: false, error: e.response?.data?.error || 'Shop API error' };
            }
            throw new AppError(ErrorCodes.NETWORK_ERROR, e.message);
        }
    },
};

const Bank = {
    async fetch() {
        try {
            const { data } = await axios.get(CFG.BANK_API_URL, { timeout: 15000 });
            if (data.code !== '00') throw new AppError(ErrorCodes.NETWORK_ERROR, 'Bank API error: ' + data.des);
            return data.transactions || [];
        } catch (e) {
            if (e instanceof AppError) throw e;
            throw new AppError(ErrorCodes.NETWORK_ERROR, e.message);
        }
    },
    parseAmount(s) { return Number(String(s).replace(/[^\d]/g, '')) || 0; },
    extractCode(desc) {
        const m = String(desc).toUpperCase().match(/NAP[A-Z0-9]{6}/);
        return m ? m[0] : null;
    },
    extractPayCode(desc) {
        const m = String(desc).toUpperCase().match(/PAY[A-Z0-9]{6}/);
        return m ? m[0] : null;
    },
    genCode() { return 'NAP' + Math.random().toString(36).slice(2, 8).toUpperCase(); },
};

const Deposit = {
    async getOrCreateCode(tid) {
        let u = await DB.getUser(tid);
        if (!u) return null;
        if (!u.depositCode) u = await DB.setDepositCode(tid, Bank.genCode());
        return u.depositCode;
    },
    async forceRefreshCode(tid) {
        const u = await DB.setDepositCode(tid, Bank.genCode());
        return u?.depositCode;
    },
    async scanAndCredit(onCredit) {
        let txs = [];
        try { txs = await Bank.fetch(); }
        catch (e) { return console.error('[Bank]', e.message); }

        for (const tx of txs) {
            if (tx.CD !== '+') continue;
            const ref = tx.Reference || `${tx.TransactionDate}-${tx.PCTime}-${tx.Amount}`;
            const amount = Bank.parseAmount(tx.Amount);
            if (amount <= 0) continue;
            const desc = String(tx.Description || '');

            const payCode = Bank.extractPayCode(desc);
            if (payCode) {
                const order = await Pending.get(payCode);
                if (order && order.status === 'pending') {
                    const lockOk = await StateStore.acquireDepositLock(`pay:${ref}`);
                    if (!lockOk) continue;
                    try {
                        const already = await DB.isDepositProcessed(ref);
                        if (already) continue;

                        if (amount >= order.amount) {
                            const res = await DB.markDepositProcessed(ref, {
                                amount, description: desc, telegramId: order.telegramId, type: 'PAY', code: payCode,
                            });
                            if (res.duplicated) continue;
                            await Pending.update(payCode, { status: 'paid', paidAmount: amount, ref });
                            if (onCredit) await onCredit({ type: 'pay', telegramId: order.telegramId, amount, order });
                        } else {
                            const res = await DB.markDepositProcessed(ref, {
                                amount, description: desc, telegramId: order.telegramId, type: 'PAY_SHORT', code: payCode,
                            });
                            if (res.duplicated) continue;
                            if (onCredit) await onCredit({ type: 'pay_short', telegramId: order.telegramId, amount, order });
                        }
                    } finally {
                        await StateStore.releaseDepositLock(`pay:${ref}`);
                    }
                    continue;
                }
            }

            const code = Bank.extractCode(desc);
            if (!code) continue;
            const user = await DB.findUserByDepositCode(code);
            if (!user) continue;

            const lockOk = await StateStore.acquireDepositLock(ref);
            if (!lockOk) continue;
            try {
                const already = await DB.isDepositProcessed(ref);
                if (already) continue;

                const markRes = await DB.markDepositProcessed(ref, {
                    amount, description: desc, telegramId: user.telegramId, type: 'NAP',
                });
                if (markRes.duplicated) continue;

                const creditRes = await DB.addBalanceAtomic(user.telegramId, amount, 'DEPOSIT', ref, {
                    bankTx: ref, description: desc,
                });
                if (creditRes.duplicated) continue;

                await DB.addTotalIn(user.telegramId, amount);
                await DB.setDepositCode(user.telegramId, Bank.genCode());

                console.log(`[Deposit] +${amount} -> ${user.telegramId}`);
                if (onCredit) await onCredit({ type: 'deposit', telegramId: user.telegramId, amount });
            } finally {
                await StateStore.releaseDepositLock(ref);
            }
        }
    },
};

const VietQR = {
    _tlv(id, value) {
        const len = String(value.length).padStart(2, '0');
        return `${id}${len}${value}`;
    },
    build({ bankBin, account, amount = 0, addInfo = '', holder }) {
        let payload = this._tlv('00', '01');
        payload += this._tlv('01', amount > 0 ? '12' : '11');
        const guid = this._tlv('00', 'A000000727');
        const org = this._tlv('00', bankBin) + this._tlv('02', 'QRIBFTTA');
        const beneficiary = this._tlv('01', org) + this._tlv('02', account);
        payload += this._tlv('38', guid + beneficiary);
        payload += this._tlv('53', '704');
        if (amount > 0) payload += this._tlv('54', String(Math.round(amount)));
        payload += this._tlv('58', 'VN');
        payload += this._tlv('59', (holder || '').slice(0, 25).toUpperCase());
        payload += this._tlv('60', 'HANOI');
        if (addInfo) payload += this._tlv('62', this._tlv('08', addInfo.slice(0, 25)));
        payload += '6304';
        payload += this._crc16(payload);
        return payload;
    },
    _crc16(str) {
        let crc = 0xFFFF;
        for (let i = 0; i < str.length; i++) {
            crc ^= str.charCodeAt(i) << 8;
            for (let j = 0; j < 8; j++) {
                crc = (crc & 0x8000) ? ((crc << 1) ^ 0x1021) : (crc << 1);
                crc &= 0xFFFF;
            }
        }
        return crc.toString(16).toUpperCase().padStart(4, '0');
    },
    async toBuffer(opts) {
        const data = this.build(opts);
        return QRCode.toBuffer(data, { type: 'png', errorCorrectionLevel: 'M', margin: 1, width: 512 });
    },
};

function getBankBin() {
    const key = String(CFG.BANK_NAME || '').trim().toUpperCase();
    return BANK_BIN[key] || BANK_BIN[key.replace(/\s*BANK$/, '')] || null;
}

const bot = new TelegramBot(CFG.BOT_TOKEN, { polling: true });

const api = axios.create({
    baseURL: `https://api.telegram.org/bot${CFG.BOT_TOKEN}`,
    timeout: 30000,
});

async function tgCall(method, payload, retries = 3) {
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            const { data } = await api.post(`/${method}`, payload);
            return data.result;
        } catch (e) {
            const status = e.response?.status;
            const retryAfter = e.response?.data?.parameters?.retry_after;

            if (status === 429 && retryAfter && attempt < retries - 1) {
                await new Promise(r => setTimeout(r, retryAfter * 1000 + 500));
                continue;
            }
            if (status >= 500 && attempt < retries - 1) {
                await new Promise(r => setTimeout(r, 2000 * (attempt + 1)));
                continue;
            }
            const desc = e.response?.data?.description || e.message;
            throw new AppError(ErrorCodes.TELEGRAM_ERROR, desc);
        }
    }
    throw new AppError(ErrorCodes.TELEGRAM_ERROR, `Max retries for ${method}`);
}

async function sendMessageRaw(chatId, text, options = {}) {
    const payload = { chat_id: chatId, text, ...options };
    return tgCall('sendMessage', payload);
}

async function editMessageRaw(chatId, messageId, text, options = {}) {
    const payload = { chat_id: chatId, message_id: messageId, text, ...options };
    return tgCall('editMessageText', payload);
}

async function sendPhotoRaw(chatId, photo, options = {}) {
    const FormData = require('form-data');
    const fd = new FormData();
    fd.append('chat_id', chatId);
    if (Buffer.isBuffer(photo)) {
        fd.append('photo', photo, { filename: 'qr.png' });
    } else {
        fd.append('photo', photo);
    }
    for (const [k, v] of Object.entries(options)) {
        if (v === undefined || v === null) continue;
        fd.append(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
    }
    try {
        const { data } = await api.post('/sendPhoto', fd, { headers: fd.getHeaders() });
        return data.result;
    } catch (e) {
        throw new AppError(ErrorCodes.TELEGRAM_ERROR, e.response?.data?.description || e.message);
    }
}

async function sendDocumentRaw(chatId, filePath, options = {}) {
    const FormData = require('form-data');
    const fd = new FormData();
    fd.append('chat_id', chatId);
    fd.append('document', fs.createReadStream(filePath));
    for (const [k, v] of Object.entries(options)) {
        if (v === undefined || v === null) continue;
        fd.append(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
    }
    try {
        const { data } = await api.post('/sendDocument', fd, { headers: fd.getHeaders() });
        return data.result;
    } catch (e) {
        throw new AppError(ErrorCodes.TELEGRAM_ERROR, e.response?.data?.description || e.message);
    }
}

async function sendDocumentBufferRaw(chatId, buffer, filename, options = {}) {
    const FormData = require('form-data');
    const fd = new FormData();
    fd.append('chat_id', chatId);
    fd.append('document', buffer, { filename });
    for (const [k, v] of Object.entries(options)) {
        if (v === undefined || v === null) continue;
        fd.append(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
    }
    try {
        const { data } = await api.post('/sendDocument', fd, { headers: fd.getHeaders() });
        return data.result;
    } catch (e) {
        throw new AppError(ErrorCodes.TELEGRAM_ERROR, e.response?.data?.description || e.message);
    }
}

let scanRequested = false;
function requestBankScan() { scanRequested = true; }

async function cleanOldMessages(chatId, userId, keep = 0) {
    if (!CFG.AUTO_CLEAN) return;
    const arr = await StateStore.getTracked(chatId, userId);
    while (arr.length > keep) {
        const mid = arr.shift();
        bot.deleteMessage(chatId, mid).catch(() => { });
    }
    await StateStore.clearTracked(chatId, userId);
    for (const mid of arr) {
        await StateStore.pushTracked(chatId, userId, mid);
    }
}

async function trackMessage(chatId, userId, msg) {
    if (!msg || !msg.message_id) return;
    await StateStore.setMsgOwner(msg.message_id, String(userId));
    if (!CFG.AUTO_CLEAN) return;
    await StateStore.pushTracked(chatId, userId, msg.message_id);
}

async function cleanAll(chatId, userId) {
    const arr = await StateStore.getTracked(chatId, userId);
    for (const mid of arr) {
        bot.deleteMessage(chatId, mid).catch(() => { });
    }
    await StateStore.clearTracked(chatId, userId);
}

function money(n) { return Number(Math.round(n)).toLocaleString('vi-VN') + 'đ'; }

function getRank(totalIn) {
    const n = Number(totalIn || 0);
    if (n >= 5_000_000) return 'VIP';
    if (n >= 1_000_000) return 'Kim cương';
    if (n >= 500_000) return 'Vàng';
    if (n >= 100_000) return 'Bạc';
    return 'Đồng';
}

function mainMenu(lang, admin = false) {
    const btn = (textKey, emojiKey) => ({
        text: t(lang, textKey),
        ...CE(emojiKey),
    });
    const kb = [
        [btn('m_products', 'products'), btn('m_account', 'id_card')],
        [btn('m_deposit', 'deposit'), btn('m_history', 'orders')],
        [btn('m_search', 'search'), btn('m_language', 'language')],
        [btn('m_support', 'support'), btn('m_help', 'help')],
    ];
    if (admin) kb.push([btn('m_admin', 'admin')]);
    return { reply_markup: { keyboard: kb, resize_keyboard: true } };
}

async function ensureUser(msg) {
    const f = msg.from;
    const name = [f.first_name, f.last_name].filter(Boolean).join(' ');
    return DB.getOrCreateUser(f.id, name, f.username);
}

async function send(chatId, text, options = {}, cleanOpt = {}) {
    const { userId = null, cleanBefore = false, editMessageId = null } = cleanOpt;
    const s = String(text ?? '');
    const hasHtml = /<(b|i|u|s|code|pre|a|tg-emoji)\b/i.test(s);
    const hasTgEmoji = /<tg-emoji/i.test(s);
    const finalOpts = hasHtml ? { parse_mode: 'HTML', ...options } : { ...options };

    if (hasTgEmoji) return safeSend(chatId, s, finalOpts, cleanOpt);

    if (editMessageId) {
        try {
            return await editMessageRaw(chatId, editMessageId, s, finalOpts);
        } catch (e) {
        }
    }

    if (cleanBefore && userId) await cleanOldMessages(chatId, userId, 0);

    const sent = await sendMessageRaw(chatId, s, finalOpts);
    if (userId) await trackMessage(chatId, userId, sent);
    return sent;
}

async function safeSend(chatId, text, options = {}, cleanOpt = {}) {
    const { userId = null, cleanBefore = false, editMessageId = null } = cleanOpt;
    const opts = options.parse_mode ? options : { parse_mode: 'HTML', ...options };

    if (editMessageId) {
        try {
            return await editMessageRaw(chatId, editMessageId, text, opts);
        } catch (e) {
        }
    }

    if (cleanBefore && userId) await cleanOldMessages(chatId, userId, 0);

    let sent = null;
    try {
        sent = await sendMessageRaw(chatId, text, opts);
    } catch (e) {
        const msg = String(e.message || '');
        const needFallback =
            msg.includes('ENTITY_TEXT_INVALID') ||
            msg.includes('parse entities') ||
            msg.includes('tg-emoji') ||
            msg.includes('custom emoji') ||
            msg.includes('CUSTOM_EMOJI');

        if (!needFallback) throw e;

        if (msg.includes('tg-emoji') || msg.includes('custom emoji') ||
            msg.includes('ENTITY_TEXT_INVALID')) {
            PREMIUM_OK = false;
        }

        try {
            const cleaned = stripPremiumEmoji(text);
            sent = await sendMessageRaw(chatId, cleaned, opts);
        } catch (e2) {
            const plain = stripPremiumEmoji(String(text)).replace(/<[^>]+>/g, '');
            const opts2 = { ...opts };
            delete opts2.parse_mode;
            sent = await sendMessageRaw(chatId, plain, opts2);
        }
    }

    if (PREMIUM_OK && sent && typeof sent.text === 'string') {
        const hasRaw = sent.text.includes('<tg-emoji') || sent.text.includes('emoji-id=');
        if (hasRaw) {
            PREMIUM_OK = false;
            try { await bot.deleteMessage(chatId, sent.message_id); } catch { }
            const cleaned = stripPremiumEmoji(text);
            sent = await sendMessageRaw(chatId, cleaned, opts);
        }
    }

    if (userId && sent) await trackMessage(chatId, userId, sent);
    return sent;
}

async function sendPhotoSafe(chatId, photo, options = {}, cleanOpt = {}) {
    const { userId = null, cleanBefore = false } = cleanOpt;
    if (cleanBefore && userId) await cleanOldMessages(chatId, userId, 0);

    const hasCaption = options.caption != null;
    const safeCaption = hasCaption
        ? (PREMIUM_OK ? options.caption : stripPremiumEmoji(options.caption))
        : undefined;

    const opts = {
        ...options,
        ...(hasCaption ? { caption: safeCaption, parse_mode: 'HTML' } : {}),
    };

    let sent = null;
    try {
        sent = await sendPhotoRaw(chatId, photo, opts);
    } catch (e) {
        const msg = String(e.message || '');
        if (msg.includes('ENTITY_TEXT_INVALID') || msg.includes('parse entities') ||
            msg.includes('tg-emoji') || msg.includes('custom emoji')) {
            if (msg.includes('tg-emoji') || msg.includes('custom emoji') || msg.includes('ENTITY_TEXT_INVALID')) {
                PREMIUM_OK = false;
            }
            const cleanedCaption = safeCaption ? stripPremiumEmoji(safeCaption) : undefined;
            sent = await sendPhotoRaw(chatId, photo, { ...options, caption: cleanedCaption });
        } else {
            throw e;
        }
    }

    if (PREMIUM_OK && sent && typeof sent.caption === 'string') {
        const hasRaw = sent.caption.includes('<tg-emoji') || sent.caption.includes('emoji-id=');
        if (hasRaw) {
            PREMIUM_OK = false;
            try { await bot.deleteMessage(chatId, sent.message_id); } catch { }
            const cleanedCaption = safeCaption ? stripPremiumEmoji(safeCaption) : undefined;
            sent = await sendPhotoRaw(chatId, photo, { ...options, caption: cleanedCaption });
        }
    }

    if (userId && sent) await trackMessage(chatId, userId, sent);
    return sent;
}

async function sendQR(chatId, userId, { amount, addInfo, caption }) {
    const bin = getBankBin();
    if (!bin) {
        await send(chatId, `${P('warn')} Chưa hỗ trợ QR cho "${CFG.BANK_NAME}".`, {}, { userId });
        return false;
    }
    try {
        const buffer = await VietQR.toBuffer({
            bankBin: bin, account: CFG.BANK_ACCOUNT,
            amount: Math.round(amount || 0), addInfo, holder: CFG.BANK_HOLDER,
        });
        await sendPhotoSafe(chatId, buffer, { caption, parse_mode: 'HTML' }, { userId });
        return true;
    } catch (e) {
        await send(chatId,
            `${P('warn')} Không tạo được QR: ${escapeHtml(e.message)}\n\n` +
            `${P('plus')} Chuyển khoản thủ công:\n${P('plus')} ${CFG.BANK_NAME}\n${P('balance')} <code>${CFG.BANK_ACCOUNT}</code>\n${P('id_card')} ${CFG.BANK_HOLDER}\n${P('pin')} <code>${addInfo}</code>${amount ? `\n${P('balance')} <b>${money(amount)}</b>` : ''}`,
            {}, { userId });
        return false;
    }
}

async function sendApiDocs(chatId, userId, headerText) {
    ensureApiDoc();

    const caption =
        `${headerText}\n\n` +
        `${P('mail')} Tài liệu đính kèm dạng Markdown, có thể đọc offline.\n` +
        `${P('chat')} Hoặc mở trang web: ${API_INFO.docsUrl}`;

    const inline = [[{ text: 'Mở trang Developer', url: API_INFO.docsUrl }]];

    await send(chatId, caption, { reply_markup: { inline_keyboard: inline } }, { userId, cleanBefore: true });

    try {
        let sent = null;
        try {
            sent = await sendDocumentRaw(chatId, API_DOC_PATH, {
                caption: `${P('mail')} <b>Tài liệu tích hợp API</b>\n${API_DOC_FILENAME}`,
                parse_mode: 'HTML',
            });
        } catch (e) {
            const plainCaption = stripPremiumEmoji(
                `${P('mail')} <b>Tài liệu tích hợp API</b>\n${API_DOC_FILENAME}`
            ).replace(/<[^>]+>/g, '');
            sent = await sendDocumentRaw(chatId, API_DOC_PATH, { caption: plainCaption });
        }

        if (userId && sent) await trackMessage(chatId, userId, sent);
    } catch (e) {
        await send(chatId, `${P('fail')} Không gửi được file.`, {}, { userId });
    }
}

async function deliverAccount(chatId, userId, user, res) {
    const isGroup = String(chatId) !== String(userId);
    const content = res.password
        ? `TÀI KHOẢN MUA TỪ SHOP BOT\n\nSản phẩm: ${res.productName}\nTài khoản: ${res.username}\nMật khẩu: ${res.password}\nMã đơn hàng: ${res.orderId}\n\nCảm ơn bạn đã mua hàng!`
        : `CHI TIẾT SẢN PHẨM TỪ SHOP BOT\n\nSản phẩm: ${res.productName}\nChi tiết: ${res.username}\nMã đơn hàng: ${res.orderId}\n\nCảm ơn bạn đã mua hàng!`;
    const filename = res.password ? `TaiKhoan_${res.orderId}.txt` : `ChiTiet_${res.orderId}.txt`;

    const plainCaption = stripPremiumEmoji(
        t(user.lang, 'buy_success', escapeHtml(res.username), escapeHtml(res.password))
    ).replace(/<[^>]+>/g, '');

    try {
        await sendDocumentBufferRaw(userId, Buffer.from(content, 'utf8'), filename, {
            caption: t(user.lang, 'buy_success', escapeHtml(res.username), escapeHtml(res.password)),
            parse_mode: 'HTML',
            ...mainMenu(user.lang, isAdmin(userId))
        });
    } catch (e) {
        try {
            await sendDocumentBufferRaw(userId, Buffer.from(content, 'utf8'), filename, {
                caption: plainCaption,
                ...mainMenu(user.lang, isAdmin(userId))
            });
        } catch (e2) {
            await send(userId, t(user.lang, 'buy_success', escapeHtml(res.username), escapeHtml(res.password)), mainMenu(user.lang, isAdmin(userId)), { userId });
        }
    }

    if (isGroup) {
        await send(chatId, t(user.lang, 'buy_success_group'), {}, { userId });
    }
}

function notifyAdmins(text) {
    ADMIN_IDS.forEach((id) => {
        sendMessageRaw(id, text, { parse_mode: 'HTML' }).catch(() => { });
    });
}

async function executePurchaseWithRecovery(internalOrderId, productId, quantity, couponCode) {
    const state = await OrderState.get(internalOrderId);
    if (!state) throw new AppError(ErrorCodes.VALIDATION_ERROR, 'Order not found');
    if (state.status === 'PURCHASE_SUCCESS') return state.snapshot;
    if (state.status === 'PURCHASE_UNKNOWN') {
        throw new AppError(ErrorCodes.SHOP_API_ERROR, 'PURCHASE_UNKNOWN');
    }

    await OrderState.update(internalOrderId, {
        status: 'PURCHASING',
        attempts: (state.attempts || 0) + 1,
    });

    let res;
    try {
        res = await ShopAPI.purchase(productId, quantity, couponCode);
    } catch (e) {
        if (e.message === 'PURCHASE_TIMEOUT' || e.code === ErrorCodes.SHOP_API_ERROR) {
            await OrderState.update(internalOrderId, { status: 'PURCHASE_UNKNOWN', last_error: e.message });
            notifyAdmins(
                `${P('warn')} <b>PURCHASE UNKNOWN</b>\nOrder: <code>${internalOrderId}</code>\n` +
                `Cần kiểm tra thủ công để tránh double purchase.`
            );
            throw new AppError(ErrorCodes.SHOP_API_ERROR, 'PURCHASE_UNKNOWN');
        }
        await OrderState.update(internalOrderId, { status: 'PURCHASE_FAILED', last_error: e.message });
        throw e;
    }

    if (!res.success) {
        await OrderState.update(internalOrderId, {
            status: 'PURCHASE_FAILED',
            last_error: res.error || 'unknown',
        });
        return res;
    }

    await OrderState.update(internalOrderId, {
        status: 'PURCHASE_SUCCESS',
        snapshot: {
            orderId: res.orderId, productName: res.productName,
            username: res.username, password: res.password,
            pricePaid: res.pricePaid,
        },
    });
    return res;
}

const H = {
    async start(chatId, userId, user) {
        await cleanAll(chatId, userId);
        const admin = isAdmin(userId);
        let text = t(user.lang, 'welcome', escapeHtml(user.name));

        const supportUrl = CFG.SUPPORT_CONTACT.startsWith('@')
            ? `https://t.me/${CFG.SUPPORT_CONTACT.slice(1)}`
            : CFG.SUPPORT_CONTACT;

        const inline = [
            [
                { text: t(user.lang, 'm_products'), callback_data: 'menu:products', ...CE('products') },
                { text: t(user.lang, 'm_account'), callback_data: 'menu:account', ...CE('id_card') }
            ],
            [
                { text: t(user.lang, 'm_deposit'), callback_data: 'menu:deposit', ...CE('deposit') },
                { text: t(user.lang, 'm_history'), callback_data: 'menu:history', ...CE('orders') }
            ],
            [
                { text: t(user.lang, 'm_search'), callback_data: 'menu:search', ...CE('search') },
                { text: t(user.lang, 'm_support'), url: supportUrl, ...CE('support') }
            ],
        ];
        if (admin) inline.push([{ text: t(user.lang, 'm_admin'), callback_data: 'admin:home', ...CE('admin') }]);

        await send(chatId, text, { reply_markup: { inline_keyboard: inline } }, { userId, cleanBefore: true });
    },

    async products(chatId, userId, user, categoryId = null, editMessageId = null) {
        let loadMsg = null;
        if (!editMessageId) {
            loadMsg = await send(chatId, t(user.lang, 'loading'), {}, { userId, cleanBefore: true });
            editMessageId = loadMsg?.message_id;
        }

        try {
            if (!categoryId) {
                const cats = await ShopAPI.getCategories();
                if (cats && cats.length > 0) {
                    const inline = cats.map(c => [{ text: c.name, callback_data: `cat:${c.id}`, ...CE('plus') }]);
                    inline.push([
                        { text: t(user.lang, 'm_search'), callback_data: 'menu:search', ...CE('search') },
                        { text: 'Làm mới', callback_data: 'menu:products', ...CE('refresh') },
                    ]);
                    return send(chatId, `${P('products')} <b>Chọn danh mục sản phẩm:</b>`, {
                        reply_markup: { inline_keyboard: inline }
                    }, { userId, editMessageId });
                }
            }

            let list = await ShopAPI.getProducts();
            if (categoryId) {
                list = list.filter(p => p.categoryId === categoryId);
            }

            if (!list.length) {
                return send(chatId, t(user.lang, 'no_products'), {
                    reply_markup: {
                        inline_keyboard: [[
                            { text: 'Quay lại', callback_data: 'menu:products', ...CE('back') },
                            { text: 'Làm mới', callback_data: categoryId ? `cat:${categoryId}` : 'menu:products', ...CE('refresh') }
                        ]]
                    }
                }, { userId, editMessageId });
            }

            const inline = list.slice(0, 20).map((p) => {
                const basePrice = Number(p.salePrice || p.price);
                const { final } = calcPrice(basePrice);
                return [{ text: `${p.name} - ${money(final)}`, callback_data: `prod:${p.id}`, ...CE('plus') }];
            });

            const bottomRow = [];
            if (categoryId) bottomRow.push({ text: 'Quay lại', callback_data: 'menu:products', ...CE('back') });
            bottomRow.push({ text: 'Làm mới', callback_data: categoryId ? `cat:${categoryId}` : 'menu:products', ...CE('refresh') });
            inline.push(bottomRow);

            await send(chatId, t(user.lang, 'product_list'), {
                reply_markup: { inline_keyboard: inline },
            }, { userId, editMessageId });
        } catch (e) {
            await send(chatId, t(user.lang, 'buy_fail', escapeHtml(e.message)), {}, { userId, editMessageId });
        }
    },

    async search(chatId, userId, user, editMessageId = null) {
        await StateStore.setCoupon(userId, { stage: 'search' });
        await send(chatId, t(user.lang, 'search_prompt'), {}, { userId, cleanBefore: !editMessageId, editMessageId });
    },

    async searchResults(chatId, userId, user, query) {
        const normalize = (s) => String(s).normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/đ/g, 'd').replace(/Đ/g, 'D')
            .toLowerCase().trim();

        const levenshtein = (a, b) => {
            if (a.length === 0) return b.length;
            if (b.length === 0) return a.length;
            const matrix = Array.from({ length: b.length + 1 }, (_, i) => [i]);
            for (let j = 0; j <= a.length; j++) matrix[0][j] = j;
            for (let i = 1; i <= b.length; i++) {
                for (let j = 1; j <= a.length; j++) {
                    if (b.charAt(i - 1) === a.charAt(j - 1)) matrix[i][j] = matrix[i - 1][j - 1];
                    else matrix[i][j] = Math.min(matrix[i - 1][j - 1] + 1, matrix[i][j - 1] + 1, matrix[i - 1][j] + 1);
                }
            }
            return matrix[b.length][a.length];
        };

        const similarity = (s1, s2) => {
            let longer = s1.length > s2.length ? s1 : s2;
            let shorter = s1.length > s2.length ? s2 : s1;
            if (longer.length === 0) return 1.0;
            return (longer.length - levenshtein(longer, shorter)) / parseFloat(longer.length);
        };

        const list = await ShopAPI.getProducts();
        const raw = query.trim();
        const qNorm = normalize(raw);
        const words = qNorm.split(/\s+/).filter(Boolean);

        const scored = list.map(p => {
            const nameNorm = normalize(p.name);
            const nameWords = nameNorm.split(/\s+/).filter(Boolean);
            let score = 0;

            if (nameNorm === qNorm) score += 100;
            else if (nameNorm.startsWith(qNorm)) score += 50;
            else if (nameNorm.includes(qNorm)) score += 30;

            const matchedWords = words.filter(w => nameNorm.includes(w));
            score += matchedWords.length * 10;
            if (matchedWords.length === words.length && words.length > 0) score += 20;

            for (const qw of words) {
                let maxSim = 0;
                for (const nw of nameWords) {
                    const sim = similarity(qw, nw);
                    if (sim > maxSim) maxSim = sim;
                }
                if (maxSim >= 0.8) {
                    score += (maxSim * 15);
                }
            }

            if (similarity(qNorm, nameNorm) >= 0.5) score += 10;

            return { p, score };
        }).filter(x => x.score > 0)
          .sort((a, b) => b.score - a.score);

        if (!scored.length) {
            return send(chatId, t(user.lang, 'search_empty', raw), {
                reply_markup: {
                    inline_keyboard: [[
                        { text: 'Tìm lại', callback_data: 'menu:search', ...CE('search') },
                    ]]
                }
            }, { userId, cleanBefore: true });
        }

        const top = scored.slice(0, 20);
        const inline = top.map(({ p }) => {
            const basePrice = Number(p.salePrice || p.price);
            const { final } = calcPrice(basePrice);
            return [{ text: `${p.name} - ${money(final)}`, callback_data: `prod:${p.id}`, ...CE('plus') }];
        });
        inline.push([
            { text: 'Tìm lại', callback_data: 'menu:search', ...CE('search') },
        ]);

        const total = scored.length > 20 ? `${scored.length} (hiển thị 20)` : scored.length;
        await send(chatId,
            t(user.lang, 'search_result', raw, total),
            { reply_markup: { inline_keyboard: inline } },
            { userId, cleanBefore: true }
        );
    },

    async productDetail(q, user, productId) {
        const chatId = q.message.chat.id;
        const userId = String(q.from.id);
        await cleanOldMessages(chatId, userId, 1);
        try {
            const list = await ShopAPI.getProducts();
            const p = list.find((x) => x.id === productId);
            if (!p) return send(chatId, t(user.lang, 'product_not_found'), {}, { userId });

            const basePrice = Number(p.salePrice || p.price);
            const { final } = calcPrice(basePrice);
            const name = escapeHtml(p.name);
            const desc = sanitizeHtml(p.description || p.tagline || '');

            const priceLine = `${P('balance')} ${t(user.lang, 'price')}: <b>${money(final)}</b>`;

            let stockInfo = '';
            let canBuy = true;
            if (p.isApiProduct === false && p.stock !== undefined) {
                if (p.stock <= 0) {
                    stockInfo = `\n${P('products')} Còn lại: <b>Hết hàng</b>`;
                    canBuy = false;
                } else {
                    stockInfo = `\n${P('products')} Còn lại: <b>${p.stock}</b>`;
                }
            }

            const caption = `<b>${name}</b>\n\n${desc}\n\n${priceLine}${stockInfo}`;

            const inline = [];
            if (canBuy) {
                inline.push([{ text: t(user.lang, 'buy_now'), callback_data: `buy:${p.id}` }]);
            } else {
                inline.push([{ text: 'Hết hàng', callback_data: `noop` }]);
            }

            if (p.thumbnail) {
                try {
                    await sendPhotoSafe(chatId, p.thumbnail, {
                        caption, parse_mode: 'HTML',
                        reply_markup: { inline_keyboard: inline },
                    }, { userId });
                } catch {
                    await send(chatId, caption, { reply_markup: { inline_keyboard: inline } }, { userId });
                }
            } else {
                await send(chatId, caption, { reply_markup: { inline_keyboard: inline } }, { userId });
            }
        } catch (e) {
            await send(chatId, t(user.lang, 'buy_fail', escapeHtml(e.message)), {}, { userId });
        }
    },

    async buyConfirm(q, user, productId) {
        const chatId = q.message.chat.id;
        const userId = String(q.from.id);
        await cleanOldMessages(chatId, userId, 1);
        try {
            const list = await ShopAPI.getProducts();
            const p = list.find((x) => x.id === productId);
            if (!p) return send(chatId, t(user.lang, 'product_not_found'), {}, { userId });

            const fresh = await DB.getUser(userId);
            if (!fresh) return send(chatId, `${P('fail')} Không tìm thấy user.`, {}, { userId });
            if (fresh.banned) return send(chatId, t(user.lang, 'banned'), {}, { userId });

            if (p.isApiProduct === false && p.stock !== undefined && p.stock <= 0) {
                return send(chatId, `${P('fail')} Sản phẩm này đã hết hàng.`, {}, { userId });
            }

            const { final } = calcPrice(Number(p.salePrice || p.price));

            await send(chatId,
                t(user.lang, 'buy_confirm', escapeHtml(p.name), money(final)),
                {
                    reply_markup: {
                        inline_keyboard: [[
                            { text: t(user.lang, 'confirm'), callback_data: `confirm:${p.id}` },
                            { text: t(user.lang, 'cancel'), callback_data: 'menu:main' },
                        ]]
                    },
                }, { userId });
        } catch (e) {
            await send(chatId, t(user.lang, 'buy_fail', escapeHtml(e.message)), {}, { userId });
        }
    },

    async buyExecute(q, user, productId) {
        const chatId = q.message.chat.id;
        const userId = String(q.from.id);
        await cleanOldMessages(chatId, userId, 1);
        try {
            const list = await ShopAPI.getProducts();
            const p = list.find((x) => x.id === productId);
            if (!p) return send(chatId, t(user.lang, 'product_not_found'), {}, { userId });

            const fresh = await DB.getUser(userId);
            if (!fresh) return send(chatId, `${P('fail')} Không tìm thấy user.`, {}, { userId });
            if (fresh.banned) return send(chatId, t(user.lang, 'banned'), {}, { userId });

            if (p.isApiProduct === false) {
                await StateStore.setCoupon(userId, { productId, stage: 'ask_qty' });
                const minQty = p.minQuantity || p.minQty || 1;
                const maxQty = p.maxQuantity || p.maxQty || 0;

                let limitText = `\n${P('pin')} Tối thiểu: ${minQty}`;
                if (maxQty > 0) limitText += ` | Tối đa: ${maxQty}`;

                const stockText = p.stock !== undefined ? ` (Hiện có: ${p.stock})` : '';
                return send(chatId, `${P('products')} <b>Nhập số lượng muốn mua:</b>\n${stockText}${limitText}`, {
                    reply_markup: {
                        inline_keyboard: [[{ text: t(user.lang, 'cancel'), callback_data: 'menu:main' }]]
                    }
                }, { userId });
            }

            await StateStore.setCoupon(userId, { productId, stage: 'ask', quantity: 1 });

            await send(chatId, t(user.lang, 'coupon_prompt'), {
                reply_markup: {
                    inline_keyboard: [[
                        { text: t(user.lang, 'coupon_skip'), callback_data: `coupon:skip:${productId}` },
                        { text: t(user.lang, 'cancel'), callback_data: 'menu:main' },
                    ]]
                },
            }, { userId });
        } catch (e) {
            await send(chatId, t(user.lang, 'buy_fail', escapeHtml(e.message)), {}, { userId });
        }
    },

    async doPurchase(chatId, userId, user, productId, couponCode = null, quantity = 1) {
        const lockToken = await StateStore.acquireBuyLock(userId, productId);
        if (!lockToken) {
            return send(chatId, t(user.lang, 'buy_processing'), {}, { userId });
        }

        let internalOrderId = null;
        try {
            const list = await ShopAPI.getProducts();
            const p = list.find((x) => x.id === productId);
            if (!p) return send(chatId, t(user.lang, 'product_not_found'), {}, { userId });

            const basePrice = Math.round(Number(p.salePrice || p.price) * quantity);
            const priceInfo = calcPrice(basePrice);
            let finalPrice = priceInfo.final;

            if (couponCode) {
                const cRes = await ShopAPI.validateAndCalcCoupon(productId, finalPrice, couponCode);
                if (!cRes.valid) return send(chatId, `${P('fail')} Lỗi mã giảm giá: ${cRes.error}`, {}, { userId });
                finalPrice = Math.max(0, finalPrice - cRes.discountAmount);
            }

            const fresh = await DB.getUser(userId);
            if (!fresh) return send(chatId, `${P('fail')} Không tìm thấy user.`, {}, { userId });
            if (fresh.banned) return send(chatId, t(user.lang, 'banned'), {}, { userId });

            if (fresh.balance < finalPrice) {
                await StateStore.setCoupon(userId, { productId, couponCode, quantity, stage: 'choose_payment' });
                return send(chatId,
                    t(user.lang, 'not_enough_choose', money(finalPrice), money(fresh.balance)),
                    {
                        reply_markup: {
                            inline_keyboard: [
                                [{ text: t(user.lang, 'pay_with_balance'), callback_data: `paybal:${productId}` }],
                                [{ text: t(user.lang, 'pay_direct'), callback_data: `paydirect:${productId}` }],
                                [{ text: t(user.lang, 'cancel'), callback_data: 'menu:main' }],
                            ]
                        },
                    }, { userId });
            }

            internalOrderId = OrderState.genId();
            await OrderState.create(internalOrderId, {
                telegramId: userId, productId,
                productName: p.name, quantity,
                price: finalPrice, basePrice: priceInfo.base, profit: priceInfo.discount,
                couponCode,
                snapshot: {
                    productName: p.name, price: finalPrice,
                    basePrice: priceInfo.base, discount: priceInfo.discount,
                    quantity, couponCode,
                },
            });

            const debited = await DB.subBalance(userId, finalPrice);
            if (!debited) {
                await OrderState.update(internalOrderId, { status: 'CANCELLED', last_error: 'insufficient_balance' });
                return send(chatId, t(user.lang, 'buy_fail', 'Không đủ số dư.'), {}, { userId });
            }

            await DB.addBalanceAtomic(userId, -finalPrice, 'PURCHASE', internalOrderId, {
                productId, productName: p.name, quantity,
            });

            await OrderState.update(internalOrderId, { status: 'PAYMENT_CONFIRMED' });

            let res;
            try {
                res = await executePurchaseWithRecovery(internalOrderId, productId, quantity, couponCode);
            } catch (e) {
                await DB.addBalanceAtomic(userId, finalPrice, 'REFUND', `refund:${internalOrderId}`, {
                    reason: e.message, orderId: internalOrderId,
                });
                await DB.addBalance(userId, finalPrice);
                await OrderState.update(internalOrderId, { status: 'REFUNDED', last_error: e.message });
                return send(chatId, t(user.lang, 'buy_fail', 'Đơn lỗi, đã hoàn tiền vào số dư.'), {}, { userId });
            }

            if (!res.success) {
                await DB.addBalanceAtomic(userId, finalPrice, 'REFUND', `refund:${internalOrderId}`, {
                    reason: res.error, orderId: internalOrderId,
                });
                await DB.addBalance(userId, finalPrice);
                await OrderState.update(internalOrderId, { status: 'REFUNDED', last_error: res.error });
                if (String(res.error || '').toLowerCase().includes('giảm giá') ||
                    String(res.error || '').toLowerCase().includes('coupon')) {
                    return send(chatId, `${P('fail')} ${escapeHtml(res.error)}\n\nThử lại không dùng mã?`, {
                        reply_markup: {
                            inline_keyboard: [[
                                { text: t(user.lang, 'btn_buy_no_code'), callback_data: `confirm:${productId}` },
                                { text: t(user.lang, 'cancel'), callback_data: 'menu:main' },
                            ]]
                        },
                    }, { userId });
                }
                return send(chatId, t(user.lang, 'buy_fail', escapeHtml(res.error || 'unknown')), {}, { userId });
            }

            await DB.addOrder(userId, {
                orderId: res.orderId, productName: res.productName,
                price: finalPrice, basePrice: priceInfo.base, profit: priceInfo.discount,
                quantity, username: res.username, password: res.password,
                status: 'success', via: 'balance',
                couponCode: couponCode || null,
            });

            await OrderState.update(internalOrderId, { status: 'DELIVERED', shopOrderId: res.orderId });

            await cleanOldMessages(chatId, userId, 0);
            await deliverAccount(chatId, userId, user, res);

            notifyAdmins(
                `${P('order2')} <b>ĐƠN MỚI (balance)</b>\n` +
                `${P('plus')} <code>${userId}</code>\n` +
                `${P('products')} ${escapeHtml(res.productName)} (x${quantity})\n` +
                `${P('balance')} Bán: ${money(finalPrice)}\n` +
                `${P('total_in')} Lợi nhuận: <b>${money(priceInfo.discount)}</b>` +
                (couponCode ? `\n${P('balance')} Coupon: <code>${escapeHtml(couponCode)}</code>` : '')
            );
        } catch (e) {
            console.error('[doPurchase]', e.code || '', e.message);
            await send(chatId, t(user.lang, 'buy_fail', escapeHtml(e.message)), {}, { userId });
        } finally {
            await StateStore.releaseBuyLock(userId, productId, lockToken);
        }
    },

    async payDirect(q, user, productId) {
        const chatId = q.message.chat.id;
        const userId = String(q.from.id);
        await cleanOldMessages(chatId, userId, 1);
        try {
            const state = (await StateStore.getCoupon(userId)) || {};
            const couponCode = state.couponCode || null;

            const list = await ShopAPI.getProducts();
            const p = list.find((x) => x.id === productId);
            if (!p) return send(chatId, t(user.lang, 'product_not_found'), {}, { userId });

            const qty = state.quantity || 1;
            const basePrice = Math.round(Number(p.salePrice || p.price) * qty);
            const priceInfo = calcPrice(basePrice);
            let finalPrice = priceInfo.final;

            if (couponCode) {
                const cRes = await ShopAPI.validateAndCalcCoupon(productId, finalPrice, couponCode);
                if (cRes.valid) {
                    finalPrice = Math.max(0, finalPrice - cRes.discountAmount);
                }
            }

            const code = 'PAY' + Math.random().toString(36).slice(2, 8).toUpperCase();
            await Pending.create({
                code, telegramId: userId, productId,
                productName: p.name, amount: finalPrice,
                basePrice: priceInfo.base, profit: priceInfo.discount,
                quantity: qty, couponCode,
            });

            requestBankScan();

            await send(chatId,
                t(user.lang, 'pay_direct_title', escapeHtml(p.name), money(finalPrice)),
                {}, { userId });

            const ok = await sendQR(chatId, userId, {
                amount: finalPrice, addInfo: code,
                caption: t(user.lang, 'pay_direct_caption', code, CFG.BANK_HOLDER, CFG.BANK_NAME, money(finalPrice)),
            });

            if (!ok) {
                await send(chatId,
                    `${P('pin')} Mã đơn: <code>${code}</code>\n` +
                    `Chuyển <b>${money(finalPrice)}</b> với nội dung <code>${code}</code>.`,
                    {}, { userId });
            }

            await send(chatId, t(user.lang, 'pay_direct_waiting'), {
                reply_markup: {
                    inline_keyboard: [[
                        { text: t(user.lang, 'btn_check'), callback_data: `paycheck:${code}` },
                        { text: t(user.lang, 'cancel'), callback_data: 'menu:main' },
                    ]]
                },
            }, { userId });
        } catch (e) {
            await send(chatId, t(user.lang, 'buy_fail', escapeHtml(e.message)), {}, { userId });
        }
    },

    async payCheck(q, user, code) {
        const chatId = q.message.chat.id;
        const userId = String(q.from.id);
        const order = await Pending.get(code);
        if (!order || order.telegramId !== userId) {
            return send(chatId, `${P('fail')} Không tìm thấy đơn.`, {}, { userId });
        }
        if (order.status === 'paid') return send(chatId, `${P('success')} Đơn đã được xử lý.`, {}, { userId });
        if (order.status === 'expired') return send(chatId, t(user.lang, 'pay_direct_expired'), {}, { userId });
        requestBankScan();
        await send(chatId, t(user.lang, 'pay_direct_waiting'), {}, { userId });
    },

    async account(chatId, userId, user, editMessageId = null) {
        const fresh = await DB.getUser(userId);
        const rank = getRank(fresh.totalIn);
        await send(chatId,
            t(user.lang, 'account', money(fresh.balance), fresh.telegramId, rank, money(fresh.totalIn || 0)),
            { reply_markup: { inline_keyboard: [[{ text: 'Làm mới', callback_data: 'menu:account', ...CE('refresh') }]] } },
            { userId, cleanBefore: !editMessageId, editMessageId });
    },

    async deposit(chatId, userId, user) {
        await cleanOldMessages(chatId, userId, 0);
        const code = await Deposit.getOrCreateCode(userId);
        const text = t(user.lang, 'deposit',
            CFG.BANK_ACCOUNT, CFG.BANK_NAME, code, CFG.BANK_HOLDER);
        await send(chatId, text, {}, { userId });

        await sendQR(chatId, userId, {
            amount: 0, addInfo: code,
            caption: `${P('pin')} <b>Quét QR để nạp tiền</b>\nMã nạp: <code>${code}</code>`,
        });

        requestBankScan();

        await send(chatId, 'Tuỳ chọn:', {
            reply_markup: {
                inline_keyboard: [[
                    { text: t(user.lang, 'btn_refresh'), callback_data: 'deposit:refresh', ...CE('refresh') },
                    { text: t(user.lang, 'btn_check'), callback_data: 'deposit:check' },
                ]],
            },
        }, { userId });
    },

    async depositRefresh(q, user) {
        const chatId = q.message.chat.id;
        const userId = String(q.from.id);
        await cleanOldMessages(chatId, userId, 1);
        const code = await Deposit.forceRefreshCode(userId);
        await send(chatId, t(user.lang, 'deposit_refreshed', code), {}, { userId });
        await sendQR(chatId, userId, {
            amount: 0, addInfo: code,
            caption: `${P('refresh')} <b>QR mới</b>\nMã: <code>${code}</code>`,
        });
        requestBankScan();
    },

    async depositCheck(q, user) {
        const chatId = q.message.chat.id;
        const userId = String(q.from.id);
        requestBankScan();
        await send(chatId, t(user.lang, 'deposit_checking'), {}, { userId });
    },

    async history(chatId, userId, user, editMessageId = null) {
        const isGroup = String(chatId) !== String(userId);
        const orders = await DB.getOrders(userId);
        if (!orders.length) return send(chatId, t(user.lang, 'history_empty'), {
            reply_markup: { inline_keyboard: [[{ text: 'Làm mới', callback_data: 'menu:history', ...CE('refresh') }]] }
        }, { userId, cleanBefore: !editMessageId, editMessageId });

        const lines = orders.slice(0, 10).map((o, i) => {
            let txt = `${i + 1}. <b>${escapeHtml(o.productName)}</b>\n` +
                `   ${P('balance')} ${money(o.price)} | ${escapeHtml(o.status)}\n`;
            if (o.password) {
                txt += `   ${P('plus')} Tài khoản: <code>${escapeHtml(o.username || '-')}</code>`;
                if (!isGroup) {
                    txt += `\n   ${P('plus')} Mật khẩu: <code>${escapeHtml(o.password)}</code>`;
                }
            } else {
                txt += `   ${P('plus')} Chi tiết SP: <code>${escapeHtml(o.username || '-')}</code>`;
            }
            return txt;
        });

        let extra = '';
        const inline_keyboard = [];
        if (isGroup) {
            extra = `\n\n<i>${P('warn')} Nhắn tin riêng cho bot và gõ /history để xem mật khẩu & tải file.</i>`;
        } else {
            orders.slice(0, 10).forEach((o, i) => {
                if (o.password) {
                    inline_keyboard.push([{ text: `Tải file đơn ${i + 1}`, callback_data: `dl_order:${o.orderId}` }]);
                }
            });
        }

        inline_keyboard.push([{ text: 'Làm mới', callback_data: 'menu:history', ...CE('refresh') }]);
        const opts = { reply_markup: { inline_keyboard } };
        await send(chatId,
            `${t(user.lang, 'history_title')}\n\n${lines.join('\n\n')}${extra}`,
            opts, { userId, cleanBefore: !editMessageId, editMessageId });
    },

    async downloadOrder(q, user, orderId) {
        const userId = String(q.from.id);
        const chatId = q.message.chat.id;
        const orders = await DB.getOrders(userId);
        const order = orders.find(o => o.orderId === orderId);
        if (!order) return send(chatId, `${P('fail')} Không tìm thấy đơn hàng.`, {}, { userId });

        await deliverAccount(chatId, userId, user, {
            productName: order.productName,
            username: order.username,
            password: order.password,
            orderId: order.orderId
        });
    },

    async language(chatId, userId, user) {
        await send(chatId, t(user.lang, 'choose_lang'), {
            reply_markup: {
                inline_keyboard: [[
                    { text: 'Tiếng Việt', callback_data: 'lang:vi' },
                    { text: 'English', callback_data: 'lang:en' },
                ]]
            },
        }, { userId, cleanBefore: true });
    },

    async setLanguage(q, user, lang) {
        if (!I18N[lang]) return;
        const userId = String(q.from.id);
        const chatId = q.message.chat.id;
        await DB.updateUser(userId, { lang });
        await cleanOldMessages(chatId, userId, 0);
        await send(chatId, t(lang, 'lang_saved'),
            mainMenu(lang, isAdmin(userId)), { userId });
    },

    async support(chatId, userId, user) {
        await send(chatId, t(user.lang, 'support', CFG.SUPPORT_CONTACT), {}, { userId, cleanBefore: true });
    },

    async help(chatId, userId, user) {
        let text =
            `${P('help')} <b>HƯỚNG DẪN SỬ DỤNG BOT</b>\n\n` +
            `<b>Lệnh người dùng</b>\n` +
            `/start — Khởi động bot\n` +
            `/products — Xem sản phẩm\n` +
            `/search — Tìm kiếm sản phẩm\n` +
            `/account — Tài khoản &amp; số dư\n` +
            `/deposit — Nạp tiền (QR tự động)\n` +
            `/history — Lịch sử mua hàng\n` +
            `/language — Đổi ngôn ngữ\n` +
            `/support — Liên hệ hỗ trợ\n` +
            `/api — Tài liệu API tích hợp\n` +
            `/help — Trợ giúp này\n\n` +
            `${P('info')} <b>Cách mua hàng</b>\n` +
            `<b>Cách 1 — Dùng số dư:</b>\n` +
            `1. Nạp tiền qua /deposit\n` +
            `2. Chuyển ĐÚNG nội dung để tự cộng tiền\n` +
            `3. Vào /products → chọn sản phẩm → Mua\n\n` +
            `<b>Cách 2 — Thanh toán trực tiếp:</b>\n` +
            `1. Vào /products → chọn sản phẩm → Mua\n` +
            `2. Nhập mã giảm giá (nếu có) hoặc Bỏ qua\n` +
            `3. Chọn "Thanh toán trực tiếp"\n` +
            `4. Quét QR và chuyển ĐÚNG số tiền + nội dung\n` +
            `5. Bot tự giao hàng sau 1-2 phút\n\n` +
            `${P('warn')} <b>Lưu ý</b>\n` +
            `• Chuyển sai nội dung → KHÔNG xử lý tự động\n` +
            `• Sản phẩm đã mua không hoàn tiền\n` +
            `• Hỗ trợ: ${CFG.SUPPORT_CONTACT}`;

        const inline = [
            [{ text: t(user.lang, 'btn_docs'), url: API_INFO.docsUrl }],
            [{ text: t(user.lang, 'btn_download_docs'), callback_data: 'api:doc' }],
        ];

        await send(chatId, text, { reply_markup: { inline_keyboard: inline } }, { userId, cleanBefore: true });
    },
};

const A = {
    home(chatId, userId) {
        return send(chatId, `${P('admin')} <b>ADMIN PANEL</b>\nChọn chức năng:`, {
            reply_markup: {
                inline_keyboard: [
                    [{ text: 'Thống kê', callback_data: 'admin:stats' },
                    { text: 'Users', callback_data: 'admin:users' }],
                    [{ text: 'Đơn hàng', callback_data: 'admin:orders' },
                    { text: 'Đơn chờ QR', callback_data: 'admin:pending' }],
                    [{ text: 'Đơn lỗi', callback_data: 'admin:stuck' },
                    { text: 'Nạp thủ công', callback_data: 'admin:manualdeposit' }],
                    [{ text: 'Tìm user', callback_data: 'admin:finduser' },
                    { text: 'Chiết khấu', callback_data: 'admin:discount' }],
                    [{ text: 'Broadcast', callback_data: 'admin:broadcast' },
                    { text: 'Cấu hình', callback_data: 'admin:config' }],
                    [{ text: 'Tải tài liệu API (.md)', callback_data: 'api:doc' }],
                    [{ text: 'Mở trang Developer API', url: API_INFO.docsUrl }],
                ],
            },
        }, { userId, cleanBefore: true });
    },

    async stuck(chatId, userId) {
        const stuck = await OrderState.findStuck(CFG.STUCK_ORDER_MIN);
        if (!stuck.length) return send(chatId, `${P('success')} Không có đơn lỗi.`, {}, { userId });

        const lines = stuck.slice(0, 15).map((o, i) => {
            const age = Math.round((Date.now() - new Date(o.updatedAt).getTime()) / 60000);
            return `${i + 1}. <code>${o.orderId}</code>\n` +
                `   ${P('plus')} User: <code>${o.telegramId}</code>\n` +
                `   ${P('products')} ${escapeHtml(o.productName || 'N/A')}\n` +
                `   Status: <b>${o.status}</b> | Age: <b>${age} min</b>\n` +
                `   Attempts: ${o.attempts || 0}${o.lastError ? `\n   Error: ${escapeHtml(o.lastError)}` : ''}`;
        }).join('\n\n');

        await send(chatId,
            `${P('warn')} <b>ĐƠN LỖI (${stuck.length})</b>\n\n${lines}\n\n` +
            `Dùng lệnh:\n<code>/refundorder ORDER_ID</code>\n<code>/completeorder ORDER_ID</code>`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    discountHome(chatId, userId) {
        const d = Settings.get().discount;
        let desc = 'Không áp dụng';
        if (d.type === 'percent' && d.value > 0) desc = `${d.value}%`;
        else if (d.type === 'fixed' && d.value > 0) desc = `${money(d.value)} / đơn`;

        return send(chatId,
            `${P('balance')} <b>CẤU HÌNH CHIẾT KHẤU</b>\n\n` +
            `Hiện tại: <b>${desc}</b>\n\n` +
            `Chiết khấu = hoa hồng admin ăn trên mỗi đơn.\n` +
            `Giá user trả = giá gốc + chiết khấu.\n\n` +
            `Chọn loại chiết khấu:`,
            {
                reply_markup: {
                    inline_keyboard: [
                        [{ text: 'Theo phần trăm (%)', callback_data: 'admin:discount:percent' }],
                        [{ text: 'Số tiền cố định (đ)', callback_data: 'admin:discount:fixed' }],
                        [{ text: 'Tắt chiết khấu', callback_data: 'admin:discount:off' }],
                        [{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }],
                    ],
                },
            }, { userId });
    },

    async discountPrompt(chatId, userId, type) {
        if (type === 'off') {
            Settings.setDiscount('none', 0);
            return send(chatId, `${P('success')} Đã tắt chiết khấu.`, {
                reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:discount', ...CE('back') }]] },
            }, { userId });
        }
        await StateStore.setAdmin(userId, { action: 'discount_value', type });
        const isPercent = type === 'percent';
        send(chatId,
            `${P('balance')} Nhập chiết khấu ${isPercent ? '<b>%</b>' : '<b>số tiền (đ)</b>'}:\n\n` +
            `VD: ${isPercent ? '10 (nghĩa là +10%)' : '5000 (nghĩa là +5.000đ/đơn)'}\n\n` +
            `Gõ /cancel để hủy.`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:discount', ...CE('back') }]] } },
            { userId });
    },

    async discountApply(chatId, userId, text) {
        const state = await StateStore.getAdmin(userId);
        if (!state || state.action !== 'discount_value') return;
        const value = Number(String(text).replace(/[^\d.]/g, ''));
        if (!value || value <= 0) return send(chatId, `${P('fail')} Giá trị không hợp lệ.`, {}, { userId });
        if (state.type === 'percent' && value > 500) {
            return send(chatId, `${P('fail')} % quá lớn (>500). Nhập lại.`, {}, { userId });
        }
        Settings.setDiscount(state.type, value);
        await StateStore.delAdmin(userId);

        const label = state.type === 'percent' ? `${value}%` : money(value);
        await send(chatId,
            `${P('success')} Đã đặt chiết khấu: <b>${label}</b>`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:discount', ...CE('back') }]] } },
            { userId });
    },

    async stats(chatId, userId) {
        const s = await DB.countUsers();
        const pendings = (await Pending.all()).filter((p) => p.status === 'pending');
        const stuck = await OrderState.findStuck(CFG.STUCK_ORDER_MIN);
        const dLabel = discountLabel();

        await send(chatId,
            `${P('rank')} <b>THỐNG KÊ</b>\n\n` +
            `${P('plus')} Users: <b>${s.total}</b>\n` +
            `${P('balance')} Tổng số dư: <b>${money(s.balanceSum)}</b>\n` +
            `${P('orders')} Đơn hàng: <b>${s.orders}</b>\n` +
            `${P('total_in')} Lợi nhuận: <b>${money(s.profit || 0)}</b>\n` +
            `${P('time')} Đơn QR chờ: <b>${pendings.length}</b>\n` +
            `${P('warn')} Đơn lỗi: <b>${stuck.length}</b>\n` +
            `${P('balance')} Chiết khấu: <b>${dLabel}</b>`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async pending(chatId, userId) {
        const all = (await Pending.all()).filter((p) => p.status === 'pending');
        if (!all.length) return send(chatId, `${P('fail')} Không có đơn chờ.`, {}, { userId });
        const lines = all.slice(0, 15).map((p, i) =>
            `${i + 1}. <code>${p.code}</code>\n` +
            `   ${P('products')} ${escapeHtml(p.productName)}\n` +
            `   ${P('balance')} ${money(p.amount)} | ${P('plus')} <code>${p.telegramId}</code>`
        ).join('\n\n');
        await send(chatId, `${P('time')} <b>ĐƠN CHỜ THANH TOÁN (${all.length})</b>\n\n${lines}`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async users(chatId, userId) {
        const users = await DB.listUsers(20);
        const lines = users.map((u, i) =>
            `${i + 1}. <b>${escapeHtml(u.name || 'NoName')}</b>${u.banned ? ' [BANNED]' : ''}\n` +
            `   ${P('plus')} <code>${u.telegramId}</code> | ${P('balance')} ${money(u.balance)} | ${P('total_in')} ${money(u.profit || 0)}`
        ).join('\n');
        await send(chatId, `${P('plus')} <b>USERS (20 mới nhất)</b>\n\n${lines || 'Trống'}`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async orders(chatId, userId) {
        const orders = await DB.listAllOrders(20);
        if (!orders.length) return send(chatId, `${P('fail')} Không có đơn hàng.`, {}, { userId });
        const lines = orders.map((o, i) =>
            `${i + 1}. <b>${escapeHtml(o.productName)}</b>\n` +
            `   ${P('id_card')} ${escapeHtml(o.userName || o.telegramId)} | ${P('balance')} ${money(o.price)} | ${escapeHtml(o.status)}` +
            (o.profit ? ` | ${P('total_in')} +${money(o.profit)}` : '')
        ).join('\n\n');
        await send(chatId, `${P('orders')} <b>ĐƠN HÀNG (20 mới nhất)</b>\n\n${lines}`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async manualDepositPrompt(chatId, userId) {
        await StateStore.setAdmin(userId, { action: 'manual_deposit' });
        send(chatId,
            `${P('deposit')} <b>NẠP THỦ CÔNG</b>\n\nNhập theo cú pháp:\n` +
            `<code>USER_ID SỐ_TIỀN</code>\n\n` +
            `Ví dụ: <code>123456789 50000</code>\n\n` +
            `Gõ /cancel để hủy.`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async manualDeposit(chatId, userId, text) {
        const parts = text.trim().split(/\s+/);
        if (parts.length < 2) return send(chatId, `${P('fail')} Sai cú pháp. VD: 123456789 50000`, {}, { userId });
        const targetId = parts[0];
        const amount = Number(parts[1].replace(/[^\d]/g, ''));
        if (!amount || amount <= 0) return send(chatId, `${P('fail')} Số tiền không hợp lệ.`, {}, { userId });

        const u = await DB.getUser(targetId);
        if (!u) return send(chatId, `${P('fail')} Không tìm thấy user <code>${targetId}</code>`, {}, { userId });

        const ref = `admin:${userId}:${Date.now()}`;
        await DB.addBalanceAtomic(targetId, amount, 'ADMIN_ADD', ref, { adminId: userId });
        await DB.addBalance(targetId, 0);
        await DB.addTotalIn(targetId, amount);
        await StateStore.delAdmin(userId);

        await send(chatId,
            `${P('success')} Đã nạp <b>${money(amount)}</b> cho user <code>${targetId}</code>`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });

        sendMessageRaw(targetId, `${P('ok')} Bạn được admin nạp <b>${money(amount)}</b>!`, { parse_mode: 'HTML' }).catch(() => { });
    },

    async findUserPrompt(chatId, userId) {
        await StateStore.setAdmin(userId, { action: 'find_user' });
        send(chatId,
            `${P('pin')} <b>TÌM USER</b>\n\nNhập Telegram ID cần tìm:\n\nGõ /cancel để hủy.`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async findUser(chatId, userId, targetId) {
        await StateStore.delAdmin(userId);
        const u = await DB.getUser(String(targetId).trim());
        if (!u) return send(chatId, `${P('fail')} Không tìm thấy user <code>${targetId}</code>`, {}, { userId });

        const orders = await DB.getOrders(u.telegramId);
        const text =
            `${P('id_card')} <b>USER INFO</b>\n\n` +
            `${P('plus')} ID: <code>${u.telegramId}</code>\n` +
            `${P('id_card')} Name: <b>${escapeHtml(u.name || 'NoName')}</b>\n` +
            `${P('chat')} Username: @${escapeHtml(u.username || 'none')}\n` +
            `${P('language')} Lang: ${u.lang}\n` +
            `${P('rank')} Rank: <b>${getRank(u.totalIn)}</b>\n` +
            `${P('balance')} Balance: <b>${money(u.balance)}</b>\n` +
            `${P('total_in')} Tổng nạp: <b>${money(u.totalIn || 0)}</b>\n` +
            `${P('total_in')} Lợi nhuận: <b>${money(u.profit || 0)}</b>\n` +
            `${P('orders')} Orders: ${orders.length}\n` +
            `${P('fail')} Banned: ${u.banned ? 'Yes' : 'No'}\n` +
            `${P('time')} Created: ${u.createdAt || '-'}`;

        await send(chatId, text, {
            reply_markup: {
                inline_keyboard: [
                    [{ text: 'Nạp tiền', callback_data: `admin:addbal:${u.telegramId}` },
                    { text: 'Trừ tiền', callback_data: `admin:subbal:${u.telegramId}` }],
                    [{ text: u.banned ? 'Mở khóa' : 'Khóa', callback_data: `admin:toggleban:${u.telegramId}` }],
                    [{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }],
                ],
            },
        }, { userId });
    },

    async balancePrompt(chatId, userId, mode, targetId) {
        await StateStore.setAdmin(userId, { action: 'balance_adjust', mode, targetId });
        send(chatId,
            `${mode === 'add' ? P('deposit') : P('total_in')} ${mode === 'add' ? 'Nạp' : 'Trừ'} tiền cho <code>${targetId}</code>\n\nNhập số tiền:\n\nGõ /cancel để hủy.`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async balanceAdjust(chatId, userId, text) {
        const state = await StateStore.getAdmin(userId);
        if (!state || state.action !== 'balance_adjust') return;
        const amount = Number(text.replace(/[^\d]/g, ''));
        if (!amount || amount <= 0) return send(chatId, `${P('fail')} Số tiền không hợp lệ.`, {}, { userId });

        const ref = `admin:${userId}:${Date.now()}`;
        if (state.mode === 'add') {
            await DB.addBalanceAtomic(state.targetId, amount, 'ADMIN_ADD', ref, { adminId: userId });
            await DB.addBalance(state.targetId, 0);
            await DB.addTotalIn(state.targetId, amount);
        } else {
            const r = await DB.subBalance(state.targetId, amount);
            if (!r) return send(chatId, `${P('fail')} User không đủ số dư.`, {}, { userId });
            await DB.addBalanceAtomic(state.targetId, -amount, 'ADMIN_SUB', ref, { adminId: userId });
            await DB.addBalance(state.targetId, 0);
        }
        await StateStore.delAdmin(userId);

        await send(chatId,
            `${P('success')} ${state.mode === 'add' ? 'Đã nạp' : 'Đã trừ'} <b>${money(amount)}</b> cho <code>${state.targetId}</code>`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async toggleBan(chatId, userId, targetId) {
        const u = await DB.getUser(targetId);
        if (!u) return send(chatId, `${P('fail')} Không tìm thấy user.`, {}, { userId });
        await DB.updateUser(targetId, { banned: !u.banned });
        await send(chatId,
            `${!u.banned ? P('fail') + ' Đã khóa' : P('success') + ' Đã mở khóa'} user <code>${targetId}</code>`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async broadcastPrompt(chatId, userId) {
        await StateStore.setAdmin(userId, { action: 'broadcast' });
        send(chatId,
            `${P('broadcast')} <b>BROADCAST</b>\n\nNhập nội dung muốn gửi tới TẤT CẢ user (hỗ trợ HTML).\n\nGõ /cancel để hủy.`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async broadcast(chatId, userId, text) {
        await StateStore.delAdmin(userId);
        const users = await DB.listUsers(999999);
        let ok = 0, fail = 0;
        await send(chatId, `${P('loading')} Đang gửi tới ${users.length} users...`, {}, { userId });

        for (const u of users) {
            try {
                await sendMessageRaw(u.telegramId, text, { parse_mode: 'HTML' });
                ok++;
            } catch { fail++; }
            await new Promise((r) => setTimeout(r, 100));
        }
        await send(chatId,
            `${P('broadcast')} <b>Broadcast xong</b>\n\n${P('success')} Thành công: ${ok}\n${P('fail')} Thất bại: ${fail}`,
            { reply_markup: { inline_keyboard: [[{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }]] } },
            { userId });
    },

    async config(chatId, userId) {
        const bin = getBankBin();
        const dLabel = discountLabel();

        const text =
            `${P('admin')} <b>CẤU HÌNH HỆ THỐNG</b>\n\n` +
            `${P('language')} Shop API: <code>${escapeHtml(CFG.SHOP_API_BASE)}</code>\n` +
            `${P('plus')} Bank: <b>${escapeHtml(CFG.BANK_NAME)}</b> (BIN: ${bin || 'không tìm thấy'})\n` +
            `${P('balance')} Account: <code>${escapeHtml(CFG.BANK_ACCOUNT)}</code>\n` +
            `${P('id_card')} Holder: <b>${escapeHtml(CFG.BANK_HOLDER)}</b>\n` +
            `${P('plus')} DB Mode: <b>${CFG.DB_MODE}</b>\n` +
            `${P('balance')} Chiết khấu: <b>${dLabel}</b>\n` +
            `${P('chat')} Support: ${escapeHtml(CFG.SUPPORT_CONTACT)}\n` +
            `${P('admin')} Admins: ${ADMIN_IDS.length}\n\n` +
            `${P('info')} <i>Token API không hiển thị ở đây. Vào trang Developer để lấy token.</i>`;

        await send(chatId, text, {
            reply_markup: {
                inline_keyboard: [
                    [{ text: 'Tải tài liệu API (.md)', callback_data: 'api:doc' }],
                    [{ text: 'Mở trang Developer API', url: API_INFO.docsUrl }],
                    [{ text: 'Quay lại', callback_data: 'admin:home', ...CE('back') }],
                ]
            },
        }, { userId });
    },
};

bot.onText(/\/start/, async (m) => { const u = await ensureUser(m); await H.start(m.chat.id, String(m.from.id), u); });
bot.onText(/\/products/, async (m) => { const u = await ensureUser(m); await H.products(m.chat.id, String(m.from.id), u); });
bot.onText(/\/search/, async (m) => { const u = await ensureUser(m); await H.search(m.chat.id, String(m.from.id), u); });
bot.onText(/\/account/, async (m) => { const u = await ensureUser(m); await H.account(m.chat.id, String(m.from.id), u); });
bot.onText(/\/deposit/, async (m) => { const u = await ensureUser(m); await H.deposit(m.chat.id, String(m.from.id), u); });
bot.onText(/\/history/, async (m) => { const u = await ensureUser(m); await H.history(m.chat.id, String(m.from.id), u); });
bot.onText(/\/language/, async (m) => { const u = await ensureUser(m); await H.language(m.chat.id, String(m.from.id), u); });
bot.onText(/\/support/, async (m) => { const u = await ensureUser(m); await H.support(m.chat.id, String(m.from.id), u); });
bot.onText(/\/help/, async (m) => { const u = await ensureUser(m); await H.help(m.chat.id, String(m.from.id), u); });

bot.onText(/\/admin/, async (m) => {
    if (!isAdmin(m.from.id)) return;
    await A.home(m.chat.id, String(m.from.id));
});

bot.onText(/\/cancel/, async (m) => {
    if (!isAdmin(m.from.id)) return;
    const userId = String(m.from.id);
    await StateStore.delAdmin(userId);
    await StateStore.delCoupon(userId);
    send(m.chat.id, `${P('fail')} Đã hủy thao tác.`, {}, { userId, cleanBefore: true });
});

bot.onText(/\/api/, async (m) => {
    const userId = String(m.from.id);
    await sendApiDocs(m.chat.id, userId, `${P('mail')} <b>TÀI LIỆU API</b>`);
});

bot.onText(/\/refundorder(?:\s+(.+))?/, async (m) => {
    if (!isAdmin(m.from.id)) return;
    const adminId = String(m.from.id);
    const orderId = (m[1] || '').trim();
    if (!orderId) return send(m.chat.id, `${P('fail')} Cú pháp: /refundorder ORDER_ID`, {}, { userId: adminId });

    const state = await OrderState.get(orderId);
    if (!state) return send(m.chat.id, `${P('fail')} Không tìm thấy order.`, {}, { userId: adminId });
    if (state.status === 'REFUNDED') return send(m.chat.id, `${P('warn')} Order đã được refund rồi.`, {}, { userId: adminId });

    const ref = `refund:${orderId}`;
    const dup = await Ledger.exists(ref, 'REFUND');
    if (dup) return send(m.chat.id, `${P('warn')} Ledger đã có refund cho order này.`, {}, { userId: adminId });

    const fresh = await DB.getUser(state.telegramId);
    if (!fresh) return send(m.chat.id, `${P('fail')} Không tìm thấy user.`, {}, { userId: adminId });

    await DB.addBalanceAtomic(state.telegramId, state.price, 'REFUND', ref, { adminId, orderId, reason: 'manual_refund' });
    await DB.addBalance(state.telegramId, 0);
    await OrderState.update(orderId, { status: 'REFUNDED', last_error: `manual_refund by ${adminId}` });

    await send(m.chat.id,
        `${P('success')} Đã refund <b>${money(state.price)}</b>\nOrder: <code>${orderId}</code>\nUser: <code>${state.telegramId}</code>`,
        {}, { userId: adminId });

    sendMessageRaw(state.telegramId,
        `${P('ok')} Đơn <code>${orderId}</code> đã được hoàn <b>${money(state.price)}</b> vào số dư.`,
        { parse_mode: 'HTML' }).catch(() => { });
});

bot.onText(/\/completeorder(?:\s+(.+))?/, async (m) => {
    if (!isAdmin(m.from.id)) return;
    const adminId = String(m.from.id);
    const orderId = (m[1] || '').trim();
    if (!orderId) return send(m.chat.id, `${P('fail')} Cú pháp: /completeorder ORDER_ID`, {}, { userId: adminId });

    const state = await OrderState.get(orderId);
    if (!state) return send(m.chat.id, `${P('fail')} Không tìm thấy order.`, {}, { userId: adminId });
    if (state.status === 'DELIVERED') return send(m.chat.id, `${P('warn')} Order đã delivered.`, {}, { userId: adminId });

    await OrderState.update(orderId, { status: 'DELIVERED', last_error: `manual_complete by ${adminId}` });
    await send(m.chat.id, `${P('success')} Đã đánh dấu DELIVERED cho <code>${orderId}</code>`, {}, { userId: adminId });
});

const USER_COMMANDS = [
    { command: 'start', description: 'Khởi động bot' },
    { command: 'products', description: 'Sản phẩm' },
    { command: 'search', description: 'Tìm kiếm sản phẩm' },
    { command: 'account', description: 'Tài khoản & số dư' },
    { command: 'deposit', description: 'Nạp tiền' },
    { command: 'history', description: 'Lịch sử mua hàng' },
    { command: 'language', description: 'Đổi ngôn ngữ' },
    { command: 'support', description: 'Hỗ trợ' },
    { command: 'api', description: 'Tài liệu API tích hợp' },
    { command: 'help', description: 'Trợ giúp' },
];
const ADMIN_COMMANDS = [
    ...USER_COMMANDS,
    { command: 'admin', description: 'Admin Panel' },
    { command: 'cancel', description: 'Hủy thao tác' },
    { command: 'refundorder', description: 'Refund đơn lỗi' },
    { command: 'completeorder', description: 'Đánh dấu đơn hoàn thành' },
];

async function setupCommands() {
    await bot.setMyCommands(USER_COMMANDS, { scope: { type: 'default' } })
        .catch((e) => console.error('[Commands default]', e.message));
    for (const adminId of ADMIN_IDS) {
        await bot.setMyCommands(ADMIN_COMMANDS, {
            scope: { type: 'chat', chat_id: Number(adminId) },
        }).catch((e) => console.error('[Commands admin]', e.message));
    }
    console.log('[Commands] Setup done');
}

bot.on('message', async (m) => {
    if (!m.text || m.text.startsWith('/')) return;

    const user = await ensureUser(m);
    const lang = user.lang;
    const text = m.text;
    const userId = String(m.from.id);
    const chatId = m.chat.id;
    const admin = isAdmin(userId);

    if (admin) {
        const state = await StateStore.getAdmin(userId);
        if (state) {
            if (state.action === 'manual_deposit') return A.manualDeposit(chatId, userId, text);
            if (state.action === 'find_user') return A.findUser(chatId, userId, text);
            if (state.action === 'balance_adjust') return A.balanceAdjust(chatId, userId, text);
            if (state.action === 'broadcast') return A.broadcast(chatId, userId, text);
            if (state.action === 'discount_value') return A.discountApply(chatId, userId, text);
        }
        if (text === t(lang, 'm_admin')) return A.home(chatId, userId);
    }

    const cState = await StateStore.getCoupon(userId);
    if (cState && cState.stage === 'ask_qty') {
        const qty = parseInt(text.trim());
        if (isNaN(qty) || qty <= 0) {
            return send(chatId, `${P('fail')} Số lượng không hợp lệ, vui lòng nhập số nguyên dương:`, {}, { userId });
        }
        const list = await ShopAPI.getProducts();
        const p = list.find(x => x.id === cState.productId);

        if (p) {
            const minQty = p.minQuantity || p.minQty || 1;
            const maxQty = p.maxQuantity || p.maxQty || 0;

            if (qty < minQty) {
                return send(chatId, `${P('fail')} Mua tối thiểu ${minQty} sản phẩm. Vui lòng nhập số lớn hơn:`, {}, { userId });
            }
            if (maxQty > 0 && qty > maxQty) {
                return send(chatId, `${P('fail')} Mua tối đa ${maxQty} sản phẩm. Vui lòng nhập lại:`, {}, { userId });
            }
            if (p.stock !== undefined && qty > p.stock) {
                return send(chatId, `${P('fail')} Kho không đủ (chỉ còn ${p.stock}). Vui lòng nhập số lượng nhỏ hơn:`, {}, { userId });
            }
        }
        await StateStore.setCoupon(userId, { productId: cState.productId, stage: 'ask', quantity: qty });
        await cleanOldMessages(chatId, userId, 0);
        return send(chatId, t(lang, 'coupon_prompt'), {
            reply_markup: {
                inline_keyboard: [[
                    { text: t(lang, 'coupon_skip'), callback_data: `coupon:skip:${cState.productId}` },
                    { text: t(lang, 'cancel'), callback_data: 'menu:main' },
                ]]
            }
        }, { userId });
    }

    if (cState && cState.stage === 'ask') {
        const code = text.trim().toUpperCase();
        if (code && /^[A-Z0-9_-]{2,32}$/.test(code)) {
            const list = await ShopAPI.getProducts();
            const p = list.find(x => x.id === cState.productId);
            if (p) {
                const qty = cState.quantity || 1;
                const bp = Number(p.salePrice || p.price) * qty;
                const priceInfo = calcPrice(bp);
                const cRes = await ShopAPI.validateAndCalcCoupon(cState.productId, priceInfo.final, code);
                if (!cRes.valid) {
                    return send(chatId, `${P('fail')} Lỗi mã: ${cRes.error}\n\n${t(lang, 'coupon_try_again')}`, {}, { userId });
                }
                await send(chatId, t(lang, 'coupon_applied', code) + ` (Giảm ${money(cRes.discountAmount)})`, {}, { userId });
            } else {
                await send(chatId, t(lang, 'coupon_applied', code), {}, { userId });
            }
            const qtyToBuy = cState.quantity || 1;
            await StateStore.delCoupon(userId);
            await cleanOldMessages(chatId, userId, 0);
            return H.doPurchase(chatId, userId, user, cState.productId, code, qtyToBuy);
        } else {
            return send(chatId, t(lang, 'coupon_try_again'), {}, { userId });
        }
    }

    if (text === t(lang, 'm_products')) return H.products(chatId, userId, user);
    if (text === t(lang, 'm_account')) return H.account(chatId, userId, user);
    if (text === t(lang, 'm_deposit')) return H.deposit(chatId, userId, user);
    if (text === t(lang, 'm_history')) return H.history(chatId, userId, user);
    if (text === t(lang, 'm_language')) return H.language(chatId, userId, user);
    if (text === t(lang, 'm_support')) return H.support(chatId, userId, user);
    if (text === t(lang, 'm_help')) return H.help(chatId, userId, user);
    if (text === t(lang, 'm_search')) return H.search(chatId, userId, user);

    if (cState && cState.stage === 'search') {
        await StateStore.delCoupon(userId);
        return H.searchResults(chatId, userId, user, text);
    }
});

bot.on('callback_query', async (q) => {
    try {
        const userId = String(q.from.id);
        const chatId = q.message.chat.id;

        if (q.message && q.message.message_id) {
            const ownerId = await StateStore.getMsgOwner(q.message.message_id);
            if (ownerId && ownerId !== userId) {
                return bot.answerCallbackQuery(q.id, { text: "Bạn không phải là người gọi lệnh này!", show_alert: true }).catch(() => { });
            }
        }

        bot.answerCallbackQuery(q.id).catch(() => { });

        const user = await DB.getOrCreateUser(
            userId,
            [q.from.first_name, q.from.last_name].filter(Boolean).join(' '),
            q.from.username
        );
        const data = q.data || '';
        const admin = isAdmin(userId);

        if (data.startsWith('lang:')) return H.setLanguage(q, user, data.split(':')[1]);
        if (data.startsWith('prod:')) return H.productDetail(q, user, data.split(':')[1]);
        if (data.startsWith('buy:')) return H.buyConfirm(q, user, data.split(':')[1]);
        if (data.startsWith('confirm:')) return H.buyExecute(q, user, data.split(':')[1]);

        if (data.startsWith('coupon:skip:')) {
            const pid = data.split(':')[2];
            const cState = (await StateStore.getCoupon(userId)) || {};
            const qty = cState.quantity || 1;
            await StateStore.delCoupon(userId);
            await cleanOldMessages(chatId, userId, 0);
            return H.doPurchase(chatId, userId, user, pid, null, qty);
        }
        if (data.startsWith('paybal:')) {
            const pid = data.split(':')[1];
            const cState = (await StateStore.getCoupon(userId)) || {};
            const coupon = cState.couponCode || null;
            const qty = cState.quantity || 1;
            await StateStore.delCoupon(userId);
            return H.doPurchase(chatId, userId, user, pid, coupon, qty);
        }
        if (data.startsWith('paydirect:')) return H.payDirect(q, user, data.split(':')[1]);
        if (data.startsWith('paycheck:')) return H.payCheck(q, user, data.split(':')[1]);
        if (data.startsWith('dl_order:')) return H.downloadOrder(q, user, data.split(':')[1]);

        if (data === 'deposit:refresh') return H.depositRefresh(q, user);
        if (data === 'deposit:check') return H.depositCheck(q, user);
        if (data === 'menu:main') {
            const fresh = await DB.getUser(userId);
            return H.start(chatId, userId, fresh);
        }
        if (data === 'menu:products') return H.products(chatId, userId, user, null, q.message?.message_id);
        if (data.startsWith('cat:')) return H.products(chatId, userId, user, data.split(':')[1], q.message?.message_id);
        if (data === 'menu:search') return H.search(chatId, userId, user, q.message?.message_id);
        if (data === 'menu:account') return H.account(chatId, userId, user, q.message?.message_id);
        if (data === 'menu:deposit') return H.deposit(chatId, userId, user);
        if (data === 'menu:history') return H.history(chatId, userId, user, q.message?.message_id);

        if (data === 'api:doc') {
            return sendApiDocs(chatId, userId, `${P('mail')} <b>TÀI LIỆU API</b>`);
        }

        if (!admin && data.startsWith('admin:')) {
            return bot.answerCallbackQuery(q.id, { text: 'Bạn không có quyền.' });
        }
        if (data === 'admin:home') return A.home(chatId, userId);
        if (data === 'admin:stats') return A.stats(chatId, userId);
        if (data === 'admin:users') return A.users(chatId, userId);
        if (data === 'admin:orders') return A.orders(chatId, userId);
        if (data === 'admin:pending') return A.pending(chatId, userId);
        if (data === 'admin:stuck') return A.stuck(chatId, userId);
        if (data === 'admin:manualdeposit') return A.manualDepositPrompt(chatId, userId);
        if (data === 'admin:finduser') return A.findUserPrompt(chatId, userId);
        if (data === 'admin:broadcast') return A.broadcastPrompt(chatId, userId);
        if (data === 'admin:config') return A.config(chatId, userId);

        if (data === 'admin:discount') return A.discountHome(chatId, userId);
        if (data === 'admin:discount:percent') return A.discountPrompt(chatId, userId, 'percent');
        if (data === 'admin:discount:fixed') return A.discountPrompt(chatId, userId, 'fixed');
        if (data === 'admin:discount:off') return A.discountPrompt(chatId, userId, 'off');

        if (data.startsWith('admin:addbal:')) return A.balancePrompt(chatId, userId, 'add', data.split(':')[2]);
        if (data.startsWith('admin:subbal:')) return A.balancePrompt(chatId, userId, 'sub', data.split(':')[2]);
        if (data.startsWith('admin:toggleban:')) return A.toggleBan(chatId, userId, data.split(':')[2]);
    } catch (e) {
        console.error('[Callback]', e.code || '', e.message);
    } finally {
        bot.answerCallbackQuery(q.id).catch(() => { });
    }
});

async function handleCredit({ type, telegramId, amount, order }) {
    const u = await DB.getUser(telegramId);
    if (!u) return;

    if (type === 'deposit') {
        try {
            await sendMessageRaw(telegramId,
                t(u.lang, 'deposit_success', money(amount)),
                { parse_mode: 'HTML', ...mainMenu(u.lang, isAdmin(telegramId)) });
        } catch (e) { console.error('[Notify]', e.code || '', e.message); }
        return;
    }

    if (type === 'pay') {
        const internalOrderId = order.code;
        try {
            const state = await OrderState.get(internalOrderId);
            if (state && state.status === 'DELIVERED') return;

            await OrderState.create(internalOrderId, {
                telegramId: String(telegramId), productId: order.productId,
                productName: order.productName,
                quantity: order.quantity || 1,
                price: Math.round(order.amount),
                basePrice: Math.round(order.basePrice || 0),
                profit: Math.round(order.profit || 0),
                couponCode: order.couponCode || null,
            });

            let res;
            try {
                res = await executePurchaseWithRecovery(internalOrderId, order.productId, order.quantity || 1, order.couponCode || null);
            } catch (e) {
                await DB.addBalanceAtomic(telegramId, order.amount, 'REFUND', `refund:${internalOrderId}`, {
                    reason: e.message, orderId: internalOrderId,
                });
                await DB.addBalance(telegramId, 0);
                await sendMessageRaw(telegramId,
                    `${t(u.lang, 'pay_direct_fail', 'Đơn lỗi, đã hoàn tiền')}\n\n${P('success')} Số tiền <b>${money(order.amount)}</b> đã được cộng vào số dư.`,
                    { parse_mode: 'HTML' });
                notifyAdmins(`${P('fail')} Lỗi đơn QR <code>${order.code}</code>: ${escapeHtml(e.message)}. Đã hoàn tiền.`);
                return;
            }

            if (!res.success) {
                await DB.addBalanceAtomic(telegramId, order.amount, 'REFUND', `refund:${internalOrderId}`, {
                    reason: res.error, orderId: internalOrderId,
                });
                await DB.addBalance(telegramId, 0);
                await sendMessageRaw(telegramId,
                    `${t(u.lang, 'pay_direct_fail', escapeHtml(res.error || 'unknown'))}\n\n${P('success')} Số tiền <b>${money(order.amount)}</b> đã được cộng vào số dư.`,
                    { parse_mode: 'HTML' });
                notifyAdmins(
                    `${P('warn')} <b>ĐƠN QR LỖI (ĐÃ HOÀN VÀO SỐ DƯ)</b>\n${P('plus')} Code: <code>${order.code}</code>\n` +
                    `${P('plus')} <code>${telegramId}</code>\n${P('balance')} ${money(order.amount)}\n${P('fail')} ${escapeHtml(res.error || '')}`
                );
                return;
            }

            await DB.addOrder(telegramId, {
                orderId: res.orderId, productName: res.productName,
                price: Math.round(order.amount), basePrice: Math.round(order.basePrice || 0),
                profit: Math.round(order.profit || 0),
                quantity: order.quantity || 1,
                username: res.username, password: res.password,
                status: 'success', via: 'direct_qr',
                couponCode: order.couponCode || null,
            });

            await DB.addTotalIn(telegramId, Math.round(order.amount));
            await OrderState.update(internalOrderId, { status: 'DELIVERED', shopOrderId: res.orderId });

            await deliverAccount(telegramId, telegramId, u, res);

            notifyAdmins(
                `${P('total_in')} <b>ĐƠN TRẢ TRỰC TIẾP</b>\n` +
                `${P('products')} ${escapeHtml(res.productName)}\n` +
                `${P('plus')} <code>${telegramId}</code>\n` +
                `${P('balance')} Bán: ${money(order.amount)}\n` +
                `${P('total_in')} Lợi nhuận: <b>${money(order.profit)}</b> (mã ${order.code})`
            );
        } catch (e) {
            console.error('[handleCredit pay]', e.code || '', e.message);
        }
        return;
    }

    if (type === 'pay_short') {
        try {
            await sendMessageRaw(telegramId,
                `${P('warn')} Bạn đã chuyển <b>${money(amount)}</b> nhưng đơn cần <b>${money(order.amount)}</b>.\n` +
                `Vui lòng chuyển thêm hoặc liên hệ hỗ trợ ${CFG.SUPPORT_CONTACT}.`,
                { parse_mode: 'HTML' });
        } catch { }
        notifyAdmins(
            `${P('warn')} <b>CHUYỂN THIẾU</b>\nCode: <code>${order.code}</code>\n` +
            `${P('plus')} <code>${telegramId}</code>\nNhận: ${money(amount)} / Cần: ${money(order.amount)}`
        );
    }
}

let scanRunning = false;

cron.schedule('*/15 * * * * *', async () => {
    if (scanRunning) return;

    const hasPending = await Pending.hasPending();
    if (!hasPending && !scanRequested) return;

    scanRequested = false;
    scanRunning = true;
    try { await Deposit.scanAndCredit(handleCredit); }
    catch (e) { console.error('[Cron bank]', e.code || '', e.message); }
    finally { scanRunning = false; }
});

cron.schedule('*/5 * * * *', async () => {
    try {
        await Pending.cleanExpired();
        const all = await Pending.all();
        for (const p of all) {
            if (p.status === 'expired' && !p.notified) {
                const u = await DB.getUser(p.telegramId);
                if (u) sendMessageRaw(p.telegramId, t(u.lang, 'pay_direct_expired'), { parse_mode: 'HTML' }).catch(() => { });
                await Pending.update(p.code, { notified: true });
            }
        }
    } catch (e) { console.error('[Cron pending]', e.message); }
});

cron.schedule('*/5 * * * *', async () => {
    try {
        const stuck = await OrderState.findStuck(CFG.STUCK_ORDER_MIN);
        for (const o of stuck) {
            if (o.notified) continue;
            const age = Math.round((Date.now() - new Date(o.updatedAt).getTime()) / 60000);
            notifyAdmins(
                `${P('warn')} <b>ĐƠN LỖI PHÁT HIỆN</b>\n` +
                `Order: <code>${o.orderId}</code>\n` +
                `User: <code>${o.telegramId}</code>\n` +
                `Status: <b>${o.status}</b>\n` +
                `Age: <b>${age} min</b>\n` +
                `Attempts: ${o.attempts || 0}` +
                (o.lastError ? `\nError: ${escapeHtml(o.lastError)}` : '') +
                `\n\nDùng /admin → Đơn lỗi để xử lý.`
            );
            await OrderState.update(o.orderId, { notified: true });
        }
    } catch (e) { console.error('[Cron stuck]', e.message); }
});

async function checkPremiumSupport() {
    if (!CFG.USE_PREMIUM_EMOJI) {
        PREMIUM_OK = false;
        return;
    }
    if (!ADMIN_IDS.length) {
        PREMIUM_OK = false;
        return;
    }
    const testId = ADMIN_IDS[0];
    const testText = `<tg-emoji emoji-id="5397916757333654639">➕</tg-emoji>`;
    try {
        const msg = await sendMessageRaw(testId, testText, { parse_mode: 'HTML' });
        const rendered = msg.text || '';
        const isRaw = rendered.includes('tg-emoji') || rendered.includes('emoji-id');
        if (isRaw) {
            PREMIUM_OK = false;
            console.log('[Premium] Raw text → OFF');
        } else {
            PREMIUM_OK = true;
            console.log('[Premium] OK');
        }
        bot.deleteMessage(testId, msg.message_id).catch(() => { });
    } catch (e) {
        PREMIUM_OK = false;
        console.log('[Premium] Test fail:', e.message, '→ OFF');
    }
}

function maskSecret(s) {
    if (!s) return '(empty)';
    if (s.length <= 8) return '***';
    return s.slice(0, 4) + '***' + s.slice(-4);
}

function validateEnv() {
    const required = ['BOT_TOKEN', 'SHOP_API_BASE', 'SHOP_API_TOKEN'];
    const missing = required.filter(k => !process.env[k]);
    if (missing.length) {
        console.error('[ENV] Missing required:', missing.join(', '));
        process.exit(1);
    }
    if (CFG.DB_MODE === 'mysql') {
        const dbReq = ['MYSQL_HOST', 'MYSQL_USER', 'MYSQL_DATABASE'];
        const miss2 = dbReq.filter(k => !process.env[k]);
        if (miss2.length) {
            console.error('[ENV] Missing MySQL:', miss2.join(', '));
            process.exit(1);
        }
    }
    console.log('[ENV] OK');
}

(async () => {
    validateEnv();
    await DB.init();
    ensureApiDoc();
    await setupCommands();
    await checkPremiumSupport();
    const bin = getBankBin();
    console.log('Bot started');
    console.log('DB Mode:', CFG.DB_MODE);
    console.log('Redis:', CFG.REDIS_ENABLED ? `ON (${CFG.REDIS.host}:${CFG.REDIS.port})` : 'OFF');
    console.log('Shop API:', CFG.SHOP_API_BASE);
    console.log('Token:', maskSecret(CFG.SHOP_API_TOKEN));
    console.log('Bank:', CFG.BANK_NAME, CFG.BANK_ACCOUNT, '| BIN:', bin || 'KHÔNG TÌM THẤY');
    console.log('Admins:', ADMIN_IDS.join(', ') || '(none)');
    console.log('Premium Emoji:', PREMIUM_OK ? 'ON' : 'OFF');
    console.log('Auto Clean:', CFG.AUTO_CLEAN ? 'ON' : 'OFF');
    console.log('Chiết khấu:', discountLabel());
})();

let shuttingDown = false;

async function gracefulShutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[Shutdown] Received ${signal}`);

    try {
        await bot.stopPolling({ cancel: true });
    } catch (e) { }

    await new Promise(r => setTimeout(r, 1000));

    try {
        if (RedisClient) await RedisClient.quit();
    } catch (e) { }

    console.log('[Shutdown] Done');
    process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

process.on('unhandledRejection', (e) => console.error('[UnhandledRejection]', e?.code || '', e?.message || e));
process.on('uncaughtException', (e) => {
    console.error('[UncaughtException]', e?.code || '', e?.message || e);
    if (e?.code === 'ECONNREFUSED' || e?.message?.includes('Redis')) return;
});

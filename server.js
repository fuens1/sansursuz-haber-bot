require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const axios = require('axios');
const cheerio = require('cheerio');
const http = require('http');
const https = require('https');
const mongoose = require('mongoose');
const multer = require('multer');
const cloudinary = require('cloudinary').v2;
const { CloudinaryStorage } = require('multer-storage-cloudinary');

// ==========================================
// 🔴 AYARLAR KISMI 
// ==========================================
const MONGO_URI = "mongodb+srv://endersener08_db_user:zF6NTp0Xjs2XMwFg@cluster0.f3whutt.mongodb.net/sansursuzhaber?appName=Cluster0";

cloudinary.config({
    cloud_name: "nhio19sg",
    api_key: "828515249182136",
    api_secret: "qyXumMq_pXy_QLcr_Yqm5XEZf38"
});
// ==========================================

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

// CLOUDINARY (Kalıcı Dosya Yükleme Ayarları)
const storage = new CloudinaryStorage({
    cloudinary: cloudinary,
    params: {
        folder: 'sansursuz_haber',
        resource_type: 'auto' 
    }
});
const upload = multer({ storage: storage });

// MONGODB (Kalıcı Veritabanı Bağlantısı)
mongoose.connect(MONGO_URI)
    .then(() => console.log('✅ Veritabanı (MongoDB) Bağlantısı Başarılı!'))
    .catch(err => console.error('❌ Veritabanı Hatası:', err));

// VERİTABANI ŞEMALARI
const ChannelSchema = new mongoose.Schema({ original: String, scrapeUrl: String, isActive: Boolean, error: Boolean });
const Channel = mongoose.model('Channel', ChannelSchema);

const NewsSchema = new mongoose.Schema({
    id: String, msgId: String, channelName: String, title: String, text: String,
    media: Object, date: String, timestamp: Number, approvedDate: String, approvedTimestamp: Number,
    views: { type: Number, default: 0 }, comments: Array, status: { type: String, enum: ['pending', 'approved'], default: 'pending' }
});
const News = mongoose.model('News', NewsSchema);

const NotifSchema = new mongoose.Schema({ id: String, msgId: String, title: String, text: String, date: String });
const Notification = mongoose.model('Notification', NotifSchema);

const SettingSchema = new mongoose.Schema({ key: String, value: mongoose.Schema.Types.Mixed });
const Setting = mongoose.model('Setting', SettingSchema);

// GLOBAL DEĞİŞKENLER
let lastMessageIds = {}; 
let isAutoFetchActive = true;
let fetchIntervalMs = 3000; 
let fetchIntervalId = null;
let sseClients = new Map();
let activeUsers = {}; 

function broadcastSSE(event, data) {
    const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    for (const res of sseClients.values()) {
        try { res.write(payload); } catch (err) {}
    }
}

const axiosInstance = axios.create({
    httpAgent: new http.Agent({ keepAlive: true }),
    httpsAgent: new https.Agent({ keepAlive: true }),
    timeout: 10000 
});

// SİSTEM BAŞLATICI (Ayarları ve geçmiş ID'leri veritabanından çeker)
async function initSystem() {
    const dbSettings = await Setting.find();
    dbSettings.forEach(s => {
        if(s.key === 'isAutoFetchActive') isAutoFetchActive = s.value;
        if(s.key === 'fetchIntervalMs') fetchIntervalMs = s.value;
    });

    const allNews = await News.find({}, 'channelName msgId');
    allNews.forEach(n => {
        if(!lastMessageIds[n.channelName]) lastMessageIds[n.channelName] = {};
        lastMessageIds[n.channelName][n.msgId] = true;
    });

    startAutoFetch();
    console.log("✅ Sistem hazır. Kalıcı Veritabanı ve Bulut Depolama Devrede!");
}
initSystem();

// ROTALAR
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/admin1762312766', (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));

app.get('/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
    const clientId = Date.now().toString() + Math.random().toString();
    sseClients.set(clientId, res);
    broadcastSSE('onlineCount', { count: sseClients.size });
    req.on('close', () => { sseClients.delete(clientId); broadcastSSE('onlineCount', { count: sseClients.size }); });
});

app.post('/ping', (req, res) => { activeUsers[req.body.userId || req.ip] = Date.now(); res.json({ success: true }); });

setInterval(() => {
    const now = Date.now();
    for(let id in activeUsers) { if(now - activeUsers[id] > 60000) delete activeUsers[id]; }
}, 15000);

app.get('/admin/stats', (req, res) => res.json({ onlineCount: Object.keys(activeUsers).length }));

app.get('/admin/auto-fetch-settings', (req, res) => res.json({ isActive: isAutoFetchActive, intervalSeconds: fetchIntervalMs / 1000 }));

app.post('/admin/auto-fetch-settings', async (req, res) => {
    const { isActive, intervalSeconds } = req.body;
    isAutoFetchActive = isActive;
    if (intervalSeconds && intervalSeconds >= 3) fetchIntervalMs = intervalSeconds * 1000;
    
    await Setting.findOneAndUpdate({ key: 'isAutoFetchActive' }, { value: isAutoFetchActive }, { upsert: true });
    await Setting.findOneAndUpdate({ key: 'fetchIntervalMs' }, { value: fetchIntervalMs }, { upsert: true });
    
    startAutoFetch();
    res.json({ success: true, isActive: isAutoFetchActive, intervalSeconds: fetchIntervalMs / 1000 });
});

app.get('/get-links', async (req, res) => {
    const links = await Channel.find();
    res.json({ data: links });
});

app.post('/add-link', async (req, res) => {
    let { link } = req.body;
    if(link && !link.includes('http')) link = 'https://' + link;
    let scrapeUrl = link.includes('t.me/') && !link.includes('t.me/s/') ? link.replace('t.me/', 't.me/s/') : link;
    
    const exists = await Channel.findOne({ original: link });
    if (!exists) await Channel.create({ original: link, scrapeUrl: scrapeUrl, isActive: true, error: false });
    
    res.json({ success: true });
});

app.post('/admin/import-channels', async (req, res) => {
    const { channels } = req.body;
    let addedCount = 0;
    if (Array.isArray(channels)) {
        for (const ch of channels) {
            const link = ch.original;
            if (link) {
                const exists = await Channel.findOne({ original: link });
                if(!exists) {
                    await Channel.create({ original: link, scrapeUrl: ch.scrapeUrl || link.replace('t.me/', 't.me/s/'), isActive: false, error: false });
                    addedCount++;
                }
            }
        }
    }
    res.json({ success: true, addedCount });
});

app.post('/toggle-status', async (req, res) => {
    await Channel.findOneAndUpdate({ original: req.body.link }, { isActive: req.body.isActive });
    res.json({ success: true });
});

app.post('/remove-link', async (req, res) => {
    await Channel.findOneAndDelete({ original: req.body.link });
    res.json({ success: true });
});

// SCRAPING YARDIMCILARI
function extractText(element) {
    let textEl = element.find('.tgme_widget_message_text');
    if (textEl.length === 0) textEl = element.find('.text-content'); 
    if (textEl.length === 0) return "";
    let clone = textEl.clone();
    clone.find('br').replaceWith('\n');
    return clone.text().trim();
}

function extractMedia(element, $, msgId) {
    let videoUrl = null; let thumbUrl = null;
    element.find('video').each(function() {
        if (videoUrl) return;
        let src = $(this).attr('src') || $(this).find('source').attr('src');
        if (src && !src.startsWith('blob:')) videoUrl = src;
    });
    element.find('[style*="background-image"]').each(function() {
        if (thumbUrl) return;
        if ($(this).closest('.tgme_widget_message_user_photo, .tgme_widget_message_text, .tgme_widget_message_author, .tgme_widget_message_info').length > 0) return;
        if ($(this).hasClass('emoji') || $(this).hasClass('tgme_widget_message_custom_emoji')) return;
        const style = $(this).attr('style');
        if (style) {
            const match = style.match(/url\(\s*(?:'|"|&quot;)?(.*?)(?:'|"|&quot;)?\s*\)/);
            if (match && match[1] && !match[1].startsWith('blob:')) thumbUrl = match[1];
        }
    });
    if (!thumbUrl) {
        element.find('img').each(function() {
            if (thumbUrl) return;
            if ($(this).closest('.tgme_widget_message_user_photo, .tgme_widget_message_text, .tgme_widget_message_author, .tgme_widget_message_info').length > 0) return;
            if ($(this).hasClass('emoji') || $(this).hasClass('Avatar__media') || $(this).hasClass('tgme_widget_message_custom_emoji')) return;
            let imgSrc = $(this).attr('src');
            if (imgSrc && !imgSrc.startsWith('blob:')) thumbUrl = imgSrc;
        });
    }

    if (videoUrl) return { type: 'video', url: videoUrl, thumb: thumbUrl || '' };
    else if (thumbUrl) return { type: 'image', url: thumbUrl };
    return null;
}

const getHeaders = () => {
    const agents = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'];
    return { 'User-Agent': agents[Math.floor(Math.random() * agents.length)], 'Cache-Control': 'no-cache, no-store, must-revalidate', 'Pragma': 'no-cache', 'Expires': '0' };
};

function parseMessageTimes($) {
    const msgs = $('.tgme_widget_message').toArray();
    let lastValidTime = 0; 
    for (let i = msgs.length - 1; i >= 0; i--) {
        const el = $(msgs[i]);
        let timeMs = 0;
        const timeTag = el.find('[datetime]').attr('datetime');
        if (timeTag) timeMs = new Date(timeTag).getTime();
        if (!timeMs || isNaN(timeMs)) {
            let textTime = el.find('.time, .message-time, .tgme_widget_message_date').text().trim();
            const match = textTime.match(/(\d{1,2}):(\d{2})/);
            if (match) {
                const hhStr = String(match[1]).padStart(2, '0');
                const mmStr = String(match[2]).padStart(2, '0');
                const todayDate = new Date(new Date().toLocaleString("en-US", {timeZone: "Europe/Istanbul"}));
                const yyyy = todayDate.getFullYear();
                const mo = String(todayDate.getMonth() + 1).padStart(2, '0');
                const da = String(todayDate.getDate()).padStart(2, '0');
                timeMs = new Date(`${yyyy}-${mo}-${da}T${hhStr}:${mmStr}:00+03:00`).getTime();
                if (timeMs > Date.now() + (5 * 60000)) timeMs -= 24 * 60 * 60 * 1000; 
            }
        }
        if ((!timeMs || isNaN(timeMs)) && lastValidTime > 0) timeMs = lastValidTime;
        if (timeMs > 0 && !isNaN(timeMs)) { lastValidTime = timeMs; el.attr('data-real-time', timeMs); } 
        else { el.attr('data-real-time', 0); }
    }
}

function generatePostLink(channelOriginal, msgId) {
    let baseLink = channelOriginal;
    if (baseLink.includes('t.me/s/')) baseLink = baseLink.replace('t.me/s/', 't.me/');
    if (!baseLink.endsWith('/')) baseLink += '/';
    return baseLink + msgId;
}

// BOT TARAMA FONKSİYONU
async function checkChannels() {
    if (!isAutoFetchActive) return;
    const activeChannels = await Channel.find({ isActive: true });
    if (activeChannels.length === 0) return;
    const limitAgo = Date.now() - (60 * 60 * 1000); 

    const requests = activeChannels.map(channel => {
        return axiosInstance.get(`${channel.scrapeUrl}?v=${Date.now()}`, { headers: getHeaders() })
            .then(async response => {
                if(channel.error) { await Channel.findByIdAndUpdate(channel._id, { error: false }); }
                return { channel, data: response.data };
            }).catch(err => null);
    });

    const results = await Promise.all(requests);
    let newPendingAdded = 0; 

    for (const result of results) {
        if (!result) continue;
        const $ = cheerio.load(result.data);
        if (!lastMessageIds[result.channel.original]) lastMessageIds[result.channel.original] = {};
        parseMessageTimes($);

        const msgs = $('.tgme_widget_message').toArray();
        for (const el of msgs) {
            const msgElement = $(el);
            const msgId = msgElement.attr('data-post');
            const msgTimeMs = parseInt(msgElement.attr('data-real-time')) || 0;
            
            if (msgTimeMs === 0 || msgTimeMs < limitAgo) continue;
            if (msgId && !lastMessageIds[result.channel.original][msgId]) {
                const msgText = extractText(msgElement);
                const mediaData = extractMedia(msgElement, $, msgId); 

                if (msgText !== "" || mediaData) {
                    lastMessageIds[result.channel.original][msgId] = true;
                    
                    await News.create({
                        id: Date.now().toString() + Math.floor(Math.random() * 10000),
                        msgId: msgId,
                        channelName: "SANSÜRSÜZ HABER", 
                        title: "",
                        postLink: generatePostLink(result.channel.original, msgId),
                        text: msgText || "",
                        media: mediaData, 
                        date: new Date(msgTimeMs).toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' }),
                        timestamp: msgTimeMs,
                        status: 'pending'
                    });
                    newPendingAdded++; 
                }
            }
        }
    }

    if (newPendingAdded > 0) broadcastSSE('newPending', { count: newPendingAdded }); 
}

function startAutoFetch() {
    if (fetchIntervalId) clearInterval(fetchIntervalId);
    if (isAutoFetchActive) fetchIntervalId = setInterval(checkChannels, fetchIntervalMs);
}

app.post('/fetch-custom-time', async (req, res) => {
    const activeChannels = await Channel.find({ isActive: true });
    if (activeChannels.length === 0) return res.json({ success: false, message: "Aktif kanal yok." });
    
    const minutes = parseInt(req.body.minutes) || 30; 
    let addedCount = 0;
    const timeLimitMs = Date.now() - (minutes * 60 * 1000);

    const requests = activeChannels.map(channel => {
        return axiosInstance.get(`${channel.scrapeUrl}?v=${Date.now()}`, { headers: getHeaders() })
            .then(response => ({ channel, data: response.data })).catch(()=>null);
    });

    const results = await Promise.all(requests);

    for (const result of results) {
        if(!result) continue;
        const $ = cheerio.load(result.data);
        if (!lastMessageIds[result.channel.original]) lastMessageIds[result.channel.original] = {};
        parseMessageTimes($);

        const msgs = $('.tgme_widget_message').toArray();
        for (const el of msgs) {
            const msgElement = $(el);
            const msgId = msgElement.attr('data-post');
            const msgTimeMs = parseInt(msgElement.attr('data-real-time')) || 0;

            if (msgTimeMs === 0 || msgTimeMs < timeLimitMs) continue;

            const msgText = extractText(msgElement);
            const mediaData = extractMedia(msgElement, $, msgId); 

            if (msgText !== "" || mediaData) {
                const exists = await News.exists({ msgId: msgId });
                if (!exists) {
                    lastMessageIds[result.channel.original][msgId] = true; 
                    await News.create({
                        id: Date.now().toString() + Math.floor(Math.random() * 10000),
                        msgId: msgId,
                        channelName: "SANSÜRSÜZ HABER", 
                        postLink: generatePostLink(result.channel.original, msgId),
                        text: msgText || "",
                        media: mediaData, 
                        date: new Date(msgTimeMs).toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' }),
                        timestamp: msgTimeMs,
                        status: 'pending'
                    });
                    addedCount++;
                }
            }
        }
    }

    if (addedCount > 0) broadcastSSE('newPending', { count: addedCount }); 
    res.json({ success: true, count: addedCount });
});

// CLOUDINARY DESTEKLİ MANUEL YÜKLEME
app.post('/admin/add-manual', upload.single('file'), async (req, res) => {
    const { title, text } = req.body;
    let mediaObj = null;

    if (req.file) {
        const isVideo = req.file.mimetype && req.file.mimetype.startsWith('video');
        mediaObj = {
            type: isVideo ? 'video' : 'image',
            url: req.file.path // Cloudinary kalıcı URL'si
        };
    }

    const nowTimestamp = Date.now();
    const nowDateString = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });

    await News.create({
        id: "MANUAL_" + Date.now().toString(),
        msgId: "manual_" + Math.random(),
        channelName: "SANSÜRSÜZ HABER",
        title: title || "", 
        text: text || "",
        media: mediaObj,
        date: nowDateString,
        timestamp: nowTimestamp,
        approvedDate: nowDateString,
        approvedTimestamp: nowTimestamp,
        status: 'approved'
    });

    broadcastSSE('refreshNews', { success: true });
    res.json({ success: true });
});

// ETKİLEŞİMLER
app.post('/interact', async (req, res) => {
    const { id, action, data } = req.body;
    const msg = await News.findOne({ id: id, status: 'approved' });
    
    if (msg) {
        if (action === 'view') {
            msg.views += 1;
            await msg.save();
        }
        if (action === 'comment' && data) {
            const commentId = "CMT_" + Date.now().toString() + Math.floor(Math.random() * 1000);
            const commentData = { id: commentId, text: data, date: new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' }) };
            
            msg.comments.push(commentData);
            await msg.save();

            await Notification.create({
                id: commentId, msgId: msg.id, title: msg.title || "İsimsiz Haber",
                text: data, date: commentData.date
            });
            broadcastSSE('refreshNews', { success: true });
        }
    }
    res.json({ success: true, msg });
});

app.post('/admin/delete-comment', async (req, res) => {
    const { msgId, commentId } = req.body;
    const msg = await News.findOne({ id: msgId });
    if (msg) {
        msg.comments = msg.comments.filter(c => c.id !== commentId);
        await msg.save();
        broadcastSSE('refreshNews', { success: true }); 
    }
    res.json({ success: true });
});

app.get('/admin/notifications', async (req, res) => {
    const notifs = await Notification.find().sort({_id: -1});
    res.json({ success: true, data: notifs });
});

app.post('/admin/clear-notifications', async (req, res) => {
    await Notification.deleteMany({});
    res.json({ success: true });
});

app.post('/admin/delete-notification', async (req, res) => {
    await Notification.findOneAndDelete({ id: req.body.id });
    res.json({ success: true });
});

// BENZERLİK VE YÖNETİM ENDPOINT'LERİ
function calculateSimilarity(str1, str2) {
    if (!str1 && !str2) return 1; 
    if (!str1 || !str2) return 0; 
    const words1 = str1.toLowerCase().replace(/[^\w\sğüşıöç]/g, '').split(/\s+/).filter(w => w.length > 2);
    const words2 = str2.toLowerCase().replace(/[^\w\sğüşıöç]/g, '').split(/\s+/).filter(w => w.length > 2);
    if(words1.length === 0 && words2.length === 0) return 1;
    if(words1.length === 0 || words2.length === 0) return 0;
    const set1 = new Set(words1);
    const set2 = new Set(words2);
    const intersection = new Set([...set1].filter(x => set2.has(x)));
    const union = new Set([...set1, ...set2]);
    return intersection.size / union.size;
}

app.post('/admin/check-similarity', async (req, res) => {
    const pending = await News.findOne({ id: req.body.id, status: 'pending' });
    if (!pending) return res.json({ isSimilar: false });

    let bestMatch = null;
    let highestScore = 0;
    
    // Son 100 haberi kontrol et (Performans için)
    const approvedMessages = await News.find({ status: 'approved' }).sort({ approvedTimestamp: -1 }).limit(100);

    for (const approved of approvedMessages) {
        let isMediaMatch = false;
        if (pending.media && approved.media && pending.media.url === approved.media.url) isMediaMatch = true;
        const textScore = calculateSimilarity(pending.text, approved.text);
        
        if (isMediaMatch || textScore > 0.45) {
            if (textScore > highestScore || isMediaMatch) {
                highestScore = isMediaMatch ? 1 : textScore;
                bestMatch = approved;
            }
        }
    }
    if (bestMatch) res.json({ isSimilar: true, similarTo: bestMatch });
    else res.json({ isSimilar: false });
});

app.get('/admin/pending', async (req, res) => {
    const limit = parseInt(req.query.limit) || 12;
    const data = await News.find({ status: 'pending' }).sort({ timestamp: -1 }).limit(limit);
    const total = await News.countDocuments({ status: 'pending' });
    res.json({ data, total });
});

app.post('/admin/delete', async (req, res) => {
    await News.findOneAndDelete({ id: req.body.id });
    res.json({ success: true });
});

app.post('/admin/edit', async (req, res) => {
    await News.findOneAndUpdate({ id: req.body.id }, { text: req.body.newText });
    res.json({ success: true });
});

app.post('/admin/approve', async (req, res) => {
    const msg = await News.findOne({ id: req.body.id, status: 'pending' });
    if (msg) {
        const nowTime = Date.now();
        const nowDateStr = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });
        
        msg.status = 'approved';
        msg.date = nowDateStr; 
        msg.timestamp = nowTime; 
        msg.approvedDate = nowDateStr;
        msg.approvedTimestamp = nowTime; 
        msg.views = 0;
        msg.comments = [];
        
        await msg.save();
        broadcastSSE('refreshNews', { success: true });
        res.json({ success: true });
    } else { res.json({ success: false }); }
});

app.get('/admin/approved', async (req, res) => {
    const limit = parseInt(req.query.limit) || 12;
    const data = await News.find({ status: 'approved' }).sort({ approvedTimestamp: -1 }).limit(limit);
    const total = await News.countDocuments({ status: 'approved' });
    res.json({ data, total });
});

app.post('/admin/approved-delete', async (req, res) => {
    await News.findOneAndDelete({ id: req.body.id });
    broadcastSSE('refreshNews', { success: true }); 
    res.json({ success: true });
});

app.post('/admin/approved-edit', async (req, res) => {
    await News.findOneAndUpdate({ id: req.body.id }, { text: req.body.newText });
    broadcastSSE('refreshNews', { success: true }); 
    res.json({ success: true });
});

app.listen(3000, () => console.log('Sunucu Aktif: http://localhost:3000'));

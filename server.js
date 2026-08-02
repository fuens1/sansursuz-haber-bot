const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const axios = require('axios');
const cheerio = require('cheerio');
const http = require('http');
const https = require('https');
const multer = require('multer');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const dir = './uploads';
        if (!fs.existsSync(dir)) fs.mkdirSync(dir);
        cb(null, dir);
    },
    filename: (req, file, cb) => {
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
        cb(null, uniqueSuffix + path.extname(file.originalname));
    }
});
const upload = multer({ storage: storage });

const CHANNELS_FILE = './channels.json';

let targetLinks = fs.existsSync(CHANNELS_FILE) ? JSON.parse(fs.readFileSync(CHANNELS_FILE, 'utf8')) : [];
let pendingMessages = [];
let approvedMessages = [];
let lastMessageIds = {}; 
let isFirstRun = true; 
let isAutoFetchActive = true;
let adminNotifications = []; 

let sseClients = new Map();

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

let activeUsers = {}; 

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    const clientId = Date.now().toString() + Math.random().toString();
    sseClients.set(clientId, res);

    broadcastSSE('onlineCount', { count: sseClients.size });

    req.on('close', () => {
        sseClients.delete(clientId);
        broadcastSSE('onlineCount', { count: sseClients.size });
    });
});

app.post('/ping', (req, res) => {
    const userId = req.body.userId || req.ip;
    activeUsers[userId] = Date.now();
    res.json({ success: true });
});

setInterval(() => {
    const now = Date.now();
    for(let id in activeUsers) {
        if(now - activeUsers[id] > 60000) delete activeUsers[id];
    }
}, 15000);

app.get('/admin/stats', (req, res) => {
    res.json({ onlineCount: Object.keys(activeUsers).length });
});

app.get('/status', (req, res) => res.json({ isAutoFetchActive }));
app.post('/toggle-autofetch', (req, res) => {
    isAutoFetchActive = req.body.isActive;
    res.json({ success: true, isActive: isAutoFetchActive });
});

app.get('/get-links', (req, res) => res.json({ data: targetLinks }));

app.post('/add-link', (req, res) => {
    let { link } = req.body;
    if(link && !link.includes('http')) link = 'https://' + link;
    let scrapeUrl = link;
    if (link && link.includes('t.me/') && !link.includes('t.me/s/')) {
        scrapeUrl = link.replace('t.me/', 't.me/s/');
    }
    if (!targetLinks.find(l => l.original === link)) {
        targetLinks.push({ original: link, scrapeUrl: scrapeUrl, isActive: true, error: false });
        fs.writeFileSync(CHANNELS_FILE, JSON.stringify(targetLinks));
    }
    res.json({ success: true, data: targetLinks });
});

// JSON İÇE AKTARMA ENDPOINT'İ
app.post('/admin/import-channels', (req, res) => {
    const { channels } = req.body;
    let addedCount = 0;
    
    if (Array.isArray(channels)) {
        channels.forEach(ch => {
            const link = ch.original;
            if (link && !targetLinks.find(l => l.original === link)) {
                targetLinks.push({ 
                    original: link, 
                    scrapeUrl: ch.scrapeUrl || link.replace('t.me/', 't.me/s/'), 
                    isActive: false, // Her zaman pasif durumda başlasın
                    error: false 
                });
                addedCount++;
            }
        });
        if(addedCount > 0) {
            fs.writeFileSync(CHANNELS_FILE, JSON.stringify(targetLinks));
        }
    }
    res.json({ success: true, addedCount });
});

app.post('/toggle-status', (req, res) => {
    const { link, isActive } = req.body;
    const channel = targetLinks.find(l => l.original === link);
    if (channel) {
        channel.isActive = isActive;
        fs.writeFileSync(CHANNELS_FILE, JSON.stringify(targetLinks));
    }
    res.json({ success: true, data: targetLinks });
});

app.post('/remove-link', (req, res) => {
    const { link } = req.body;
    targetLinks = targetLinks.filter(l => l.original !== link);
    fs.writeFileSync(CHANNELS_FILE, JSON.stringify(targetLinks));
    res.json({ success: true, data: targetLinks });
});

function extractText(element) {
    let textEl = element.find('.tgme_widget_message_text');
    if (textEl.length === 0) textEl = element.find('.text-content'); 
    if (textEl.length === 0) return "";
    let clone = textEl.clone();
    clone.find('br').replaceWith('\n');
    return clone.text().trim();
}

function extractMedia(element, $, msgId) {
    let videoUrl = null;
    let thumbUrl = null;

    element.find('video').each(function() {
        if (videoUrl) return;
        let src = $(this).attr('src');
        if (!src) src = $(this).find('source').attr('src');
        if (src && !src.startsWith('blob:')) videoUrl = src;
    });

    element.find('[style*="background-image"]').each(function() {
        if (thumbUrl) return;
        if ($(this).closest('.tgme_widget_message_user_photo, .tgme_widget_message_text, .tgme_widget_message_author, .tgme_widget_message_info').length > 0) return;
        if ($(this).hasClass('emoji') || $(this).hasClass('tgme_widget_message_custom_emoji')) return;

        const style = $(this).attr('style');
        if (style) {
            const match = style.match(/url\(\s*(?:'|"|&quot;)?(.*?)(?:'|"|&quot;)?\s*\)/);
            if (match && match[1] && !match[1].startsWith('blob:')) {
                thumbUrl = match[1];
            }
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

    const isVideoNode = element.find('.tgme_widget_message_video_player, .tgme_widget_message_video_thumb, .tgme_widget_message_video_icon, .tgme_widget_message_video_wrap, i.icon-large-play, video, .message-media-duration, .tgme_widget_message_video_duration, [class*="video"]').length > 0;

    if (videoUrl) return { type: 'video', url: videoUrl, thumb: thumbUrl || '' };
    else if (isVideoNode && msgId) return { type: 'iframe', url: `https://t.me/${msgId}?embed=1&dark=1` };
    else if (thumbUrl) return { type: 'image', url: thumbUrl };
    
    return null;
}

const getHeaders = () => {
    const agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
    ];
    return {
        'User-Agent': agents[Math.floor(Math.random() * agents.length)],
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
    };
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
                const todayStr = new Date().toLocaleString("en-US", {timeZone: "Europe/Istanbul"});
                const todayDate = new Date(todayStr);
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

async function checkChannels() {
    if (!isAutoFetchActive) return;
    const activeChannels = targetLinks.filter(c => c.isActive !== false);
    if (activeChannels.length === 0) return;
    const limitAgo = Date.now() - (60 * 60 * 1000); 

    const requests = activeChannels.map(channel => {
        const bypassCacheUrl = `${channel.scrapeUrl}?v=${Date.now()}`;
        return axiosInstance.get(bypassCacheUrl, { headers: getHeaders() })
            .then(response => {
                if(channel.error) { channel.error = false; fs.writeFileSync(CHANNELS_FILE, JSON.stringify(targetLinks)); }
                return { channel, data: response.data };
            }).catch(err => null);
    });

    const results = await Promise.all(requests);
    let newPendingAdded = 0; 

    results.forEach(result => {
        if (!result) return;
        const $ = cheerio.load(result.data);
        if (!lastMessageIds[result.channel.original]) lastMessageIds[result.channel.original] = {};
        parseMessageTimes($);

        $('.tgme_widget_message').each((i, el) => {
            const msgElement = $(el);
            const msgId = msgElement.attr('data-post');
            const msgTimeMs = parseInt(msgElement.attr('data-real-time')) || 0;
            
            if (msgTimeMs === 0 || msgTimeMs < limitAgo) return;
            if (msgId && !lastMessageIds[result.channel.original][msgId]) {
                const msgText = extractText(msgElement);
                const mediaData = extractMedia(msgElement, $, msgId); 

                if (msgText !== "" || mediaData) {
                    lastMessageIds[result.channel.original][msgId] = true;
                    if (!isFirstRun) {
                        pendingMessages.push({
                            id: Date.now().toString() + Math.floor(Math.random() * 10000),
                            msgId: msgId,
                            channelName: "SANSÜRSÜZ HABER", 
                            postLink: generatePostLink(result.channel.original, msgId),
                            text: msgText || "",
                            media: mediaData, 
                            date: new Date(msgTimeMs).toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' }),
                            timestamp: msgTimeMs 
                        });
                        newPendingAdded++; 
                    }
                }
            }
        });
    });

    if (newPendingAdded > 0 && !isFirstRun) {
        broadcastSSE('newPending', { count: newPendingAdded }); 
    }

    if (isFirstRun) { console.log("✅ Sistem hazır. Kalıcı Oturum ve Push Bildirimler Aktif!"); isFirstRun = false; }
}
setInterval(checkChannels, 3000);
setTimeout(checkChannels, 1000);

app.post('/fetch-custom-time', async (req, res) => {
    const activeChannels = targetLinks.filter(c => c.isActive !== false);
    if (activeChannels.length === 0) return res.json({ success: false, message: "Aktif kanal yok." });
    
    const minutes = parseInt(req.body.minutes) || 30; 
    let addedCount = 0;
    const timeLimitMs = Date.now() - (minutes * 60 * 1000);

    const requests = activeChannels.map(channel => {
        const bypassCacheUrl = `${channel.scrapeUrl}?v=${Date.now()}`;
        return axiosInstance.get(bypassCacheUrl, { headers: getHeaders() })
            .then(response => ({ channel, data: response.data }))
            .catch(()=>null);
    });

    const results = await Promise.all(requests);

    results.forEach(result => {
        if(!result) return;
        const $ = cheerio.load(result.data);
        if (!lastMessageIds[result.channel.original]) lastMessageIds[result.channel.original] = {};

        parseMessageTimes($);

        $('.tgme_widget_message').each((i, el) => {
            const msgElement = $(el);
            const msgId = msgElement.attr('data-post');
            const msgTimeMs = parseInt(msgElement.attr('data-real-time')) || 0;

            if (msgTimeMs === 0 || msgTimeMs < timeLimitMs) return;

            const msgText = extractText(msgElement);
            const mediaData = extractMedia(msgElement, $, msgId); 

            if (msgText !== "" || mediaData) {
                const existsPending = pendingMessages.some(m => m.msgId === msgId);
                const existsApproved = approvedMessages.some(m => m.msgId === msgId);
                
                if (!existsPending && !existsApproved) {
                    pendingMessages.push({
                        id: Date.now().toString() + Math.floor(Math.random() * 10000),
                        msgId: msgId,
                        channelName: "SANSÜRSÜZ HABER", 
                        postLink: generatePostLink(result.channel.original, msgId),
                        text: msgText || "",
                        media: mediaData, 
                        date: new Date(msgTimeMs).toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' }),
                        timestamp: msgTimeMs 
                    });
                    lastMessageIds[result.channel.original][msgId] = true; 
                    addedCount++;
                }
            }
        });
    });

    if (addedCount > 0) {
        broadcastSSE('newPending', { count: addedCount }); 
    }

    res.json({ success: true, count: addedCount });
});

app.post('/admin/add-manual', upload.single('file'), (req, res) => {
    const { title, text } = req.body;
    let mediaObj = null;

    if (req.file) {
        const isVideo = req.file.mimetype.startsWith('video/');
        mediaObj = {
            type: isVideo ? 'video' : 'image',
            url: `/uploads/${req.file.filename}`
        };
    }

    const nowTimestamp = Date.now();
    const nowDateString = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });

    const newMsg = {
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
        views: 0,
        comments: []
    };
    approvedMessages.unshift(newMsg); 

    broadcastSSE('refreshNews', { success: true });

    res.json({ success: true });
});

app.post('/interact', (req, res) => {
    const { id, action, data } = req.body;
    const msg = approvedMessages.find(m => m.id === id);
    
    if (msg) {
        if (action === 'view') msg.views = (msg.views || 0) + 1;
        if (action === 'comment' && data) {
            if (!msg.comments) msg.comments = [];
            const commentId = "CMT_" + Date.now().toString() + Math.floor(Math.random() * 1000);
            
            const commentData = { 
                id: commentId, 
                text: data, 
                date: new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' }) 
            };
            msg.comments.push(commentData);

            adminNotifications.unshift({
                id: commentId,
                msgId: msg.id,
                title: msg.title || "İsimsiz Haber",
                text: data,
                date: commentData.date
            });
            broadcastSSE('refreshNews', { success: true });
        }
    }
    res.json({ success: true, msg });
});

app.post('/admin/delete-comment', (req, res) => {
    const { msgId, commentId } = req.body;
    const msg = approvedMessages.find(m => m.id === msgId);
    
    if (msg && msg.comments) {
        msg.comments = msg.comments.filter(c => c.id !== commentId);
        broadcastSSE('refreshNews', { success: true }); 
    }
    res.json({ success: true });
});

app.get('/admin/notifications', (req, res) => {
    res.json({ success: true, data: adminNotifications });
});

app.post('/admin/clear-notifications', (req, res) => {
    adminNotifications = [];
    res.json({ success: true });
});

app.post('/admin/delete-notification', (req, res) => {
    adminNotifications = adminNotifications.filter(n => n.id !== req.body.id);
    res.json({ success: true });
});

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

app.post('/admin/check-similarity', (req, res) => {
    const { id } = req.body;
    const pending = pendingMessages.find(m => m.id === id);
    if (!pending) return res.json({ isSimilar: false });

    let bestMatch = null;
    let highestScore = 0;

    for (const approved of approvedMessages) {
        let isMediaMatch = false;
        if (pending.media && approved.media && pending.media.url === approved.media.url) {
            isMediaMatch = true;
        }
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

app.get('/admin/pending', (req, res) => {
    pendingMessages.sort((a, b) => b.timestamp - a.timestamp);
    const limit = parseInt(req.query.limit) || 12;
    res.json({ data: pendingMessages.slice(0, limit), total: pendingMessages.length });
});

app.post('/admin/delete', (req, res) => {
    pendingMessages = pendingMessages.filter(m => m.id !== req.body.id);
    res.json({ success: true });
});

app.post('/admin/edit', (req, res) => {
    const msg = pendingMessages.find(m => m.id === req.body.id);
    if (msg) msg.text = req.body.newText;
    res.json({ success: true });
});

app.post('/admin/approve', (req, res) => {
    const { id } = req.body;
    const msgIndex = pendingMessages.findIndex(m => m.id === id);
    if (msgIndex > -1) {
        const msg = pendingMessages.splice(msgIndex, 1)[0]; 
        
        const nowTime = Date.now();
        const nowDateStr = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });
        
        msg.date = nowDateStr; 
        msg.timestamp = nowTime; 
        
        msg.approvedDate = nowDateStr;
        msg.approvedTimestamp = nowTime; 
        
        msg.views = 0;
        msg.comments = [];
        
        approvedMessages.unshift(msg); 
        
        broadcastSSE('refreshNews', { success: true });

        res.json({ success: true });
    } else { res.json({ success: false }); }
});

app.get('/admin/approved', (req, res) => {
    approvedMessages.sort((a, b) => b.approvedTimestamp - a.approvedTimestamp);
    const limit = parseInt(req.query.limit) || 12;
    res.json({ data: approvedMessages.slice(0, limit), total: approvedMessages.length });
});

app.post('/admin/approved-delete', (req, res) => {
    approvedMessages = approvedMessages.filter(m => m.id !== req.body.id);
    broadcastSSE('refreshNews', { success: true }); 
    res.json({ success: true });
});

app.post('/admin/approved-edit', (req, res) => {
    const msg = approvedMessages.find(m => m.id === req.body.id);
    if (msg) msg.text = req.body.newText;
    broadcastSSE('refreshNews', { success: true }); 
    res.json({ success: true });
});

app.listen(3000, () => console.log('Sunucu Aktif: http://localhost:3000'));

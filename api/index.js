// /api/index.js (النسخة النهائية والمستقرة مع قراءة تدفق البيانات الآمنة)

/**
 * SHIB Ads WebApp Backend API
 * Handles all POST requests from the Telegram Mini App frontend.
 * Uses the Supabase REST API for persistence.
 */
const crypto = require('crypto');

// Load environment variables for Supabase connection
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
// ⚠️ BOT_TOKEN يجب أن يتم ضبطه في متغيرات بيئة Vercel
const BOT_TOKEN = process.env.BOT_TOKEN;

// ------------------------------------------------------------------
// ثوابت جانب الخادم المؤمنة والمحددة
// ------------------------------------------------------------------
const REWARD_PER_AD = 3;
const REFERRAL_COMMISSION_RATE = 0.05;
const DAILY_MAX_ADS = 100; // الحد الأقصى للإعلانات
const DAILY_MAX_SPINS = 15; // الحد الأقصى للدورات (Spins)
const RESET_INTERVAL_MS = 6 * 60 * 60 * 1000; // ⬅️ 6 ساعات بالمللي ثانية
const MIN_TIME_BETWEEN_ACTIONS_MS = 3000; // الحد الأدنى للوقت بين الطلبات (3 ثوانٍ)
const ACTION_ID_EXPIRY_MS = 60000; // صلاحية معرف الإجراء (60 ثانية)
const SPIN_SECTORS = [5, 10, 15, 20, 5];
const TASK_REWARD = 200; // المكافأة الافتراضية للمهام

// ------------------------------------------------------------------
// المساعدات الأمنية والمنفعة (Security and Utility Helpers)
// ------------------------------------------------------------------

/**
 * إرسال استجابة JSON ناجحة.
 */
function sendSuccess(res, data, status = 200) {
    res.status(status).json({ ok: true, data });
}

/**
 * إرسال استجابة JSON خطأ.
 */
function sendError(res, message, status = 400, errorType = 'error', errorTitle = 'Operation Failed') {
    res.status(status).json({ 
        ok: false, 
        error: message, 
        errorType: errorType,
        errorTitle: errorTitle,
        cleanMessage: message.split(':')[0] 
    });
}

/**
 * التحقق من صحة بيانات initData لتطبيق Telegram Mini App.
 */
function validateInitData(initData) {
    // 🛑🛑🛑 الإصلاح هنا: التحقق الصارم قبل المعالجة لتجنب TypeError 🛑🛑🛑
    if (!initData || typeof initData !== 'string') return false;
    
    const urlParams = new URLSearchParams(initData);
    const hash = urlParams.get('hash');
    const params = [];
    
    // جمع جميع المعلمات باستثناء 'hash' وفرزها أبجدياً
    for (const [key, value] of urlParams.entries()) {
        if (key !== 'hash') {
            params.push(`${key}=${value}`);
        }
    }
    
    if (params.length === 0) return false;

    const dataCheckString = params.sort().join('\n');
    
    // تشفير توكن البوت للحصول على المفتاح السري
    const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    
    // حساب الهاش للتحقق
    const computedHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex'); 
    
    return computedHash === hash;
}

/**
 * دالة عامة لجلب البيانات من Supabase باستخدام REST API.
 */
async function supabaseFetch(table, method, body = null, urlParams = '') {
    const url = `${SUPABASE_URL}/rest/v1/${table}${urlParams}`;
    
    const headers = {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=representation' 
    };

    const config = {
        method: method,
        headers: headers,
    };

    if (body) {
        config.body = JSON.stringify(body);
    }
    
    const response = await fetch(url, config);
    
    if (response.status === 204 && (method === 'PATCH' || method === 'DELETE')) {
        return []; 
    }

    if (!response.ok) {
        let errorBody = await response.text();
        try {
            errorBody = JSON.parse(errorBody);
        } catch (e) {
            // تجاهل خطأ تحليل JSON
        }
        const errorDetail = typeof errorBody === 'object' && errorBody.message ? errorBody.message : response.statusText;
        throw new Error(`Supabase error (${response.status} ${table}): ${errorDetail}`);
    }

    return response.json();
}


/**
 * التحقق مما إذا كان المستخدم عضواً في قناة Telegram (لفحص المهام).
 */
async function checkChannelMembership(userId, channelUsername) {
    let chatUsername = channelUsername.replace('https://t.me/', '');
    chatUsername = chatUsername.startsWith('@') ? chatUsername : `@${chatUsername}`;
    
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`;

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: chatUsername,
                user_id: userId
            })
        });

        if (!response.ok) {
            console.error(`Telegram API error for ${chatUsername}: ${response.statusText}`);
            return false;
        }

        const data = await response.json();

        if (data.ok) {
            const status = data.result.status;
            // حالات العضوية المقبولة
            return ['member', 'administrator', 'creator'].includes(status);
        } else {
            console.error('Telegram API responded with error:', data.description);
            return false; 
        }

    } catch (error) {
        console.error('Network error during checkChannelMembership:', error.message);
        return false;
    }
}


// ------------------------------------------------------------------
// مساعدات معرف الإجراء ومكافحة الرسائل المتكررة (Action ID and Anti-Spam Helpers)
// ------------------------------------------------------------------

const actionIdStore = new Map();

function generateActionId(user_id, type) {
    const id = crypto.randomBytes(16).toString('hex');
    actionIdStore.set(id, {
        user_id: user_id,
        type: type,
        timestamp: Date.now()
    });
    // يتم تنظيف المعرفات المنتهية الصلاحية بشكل دوري
    setTimeout(cleanupActionIds, ACTION_ID_EXPIRY_MS + 5000); 
    return id;
}

function consumeActionId(id, user_id, type) {
    const record = actionIdStore.get(id);

    if (!record || record.user_id !== user_id || record.type !== type) {
        return false;
    }
    if (Date.now() - record.timestamp > ACTION_ID_EXPIRY_MS) {
        actionIdStore.delete(id); 
        return false;
    }

    actionIdStore.delete(id);
    return true;
}

function cleanupActionIds() {
    const now = Date.now();
    for (const [id, record] of actionIdStore.entries()) {
        if (now - record.timestamp > ACTION_ID_EXPIRY_MS) {
            actionIdStore.delete(id);
        }
    }
}

function isCooldownActive(lastActivity) {
    if (!lastActivity) return false;
    const lastActivityTime = new Date(lastActivity).getTime();
    return (Date.now() - lastActivityTime) < MIN_TIME_BETWEEN_ACTIONS_MS;
}

// ------------------------------------------------------------------
// معالجات الطلبات (Request Handlers)
// ------------------------------------------------------------------

/**
 * 1) type: "getUserData" - جلب بيانات المستخدم أو تسجيله.
 */
async function handleGetUserData(req, res, body) {
    const { user_id, telegram_username, first_name, photo_url } = body;
    const id = parseInt(user_id);
    const dateNow = new Date().toISOString();

    try {
        let users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=*`);
        let user = users.length > 0 ? users[0] : null;

        if (!user) {
            const referralId = body.referral_id ? parseInt(body.referral_id) : null;
            
            if (referralId && referralId === id) {
                return sendError(res, 'Invalid referral ID: Cannot self-refer.', 400);
            }

            let referrer = null;
            if (referralId) {
                const referrers = await supabaseFetch('users', 'GET', null, `?id=eq.${referralId}&select=id`);
                referrer = referrers.length > 0 ? referrers[0] : null;
            }

            const newUserPayload = {
                id: id,
                telegram_username: telegram_username,
                first_name: first_name,
                photo_url: photo_url,
                balance: 0,
                referral_id: referrer ? referrer.id : null,
                last_ad_watch: dateNow,
                last_spin: dateNow,
                last_activity: dateNow,
                daily_ads_watched: 0,
                daily_spins: 0,
                reset_timestamp: new Date(Date.now() + RESET_INTERVAL_MS).toISOString()
            };
            
            const [registeredUser] = await supabaseFetch('users', 'POST', newUserPayload);
            user = registeredUser;

            return sendSuccess(res, { 
                user: user, 
                message: 'User registered successfully.',
                is_new: true
            });

        } else {
            const currentTime = Date.now();
            const resetTime = new Date(user.reset_timestamp).getTime();
            let shouldReset = false;

            if (currentTime >= resetTime) {
                shouldReset = true;
                user.reset_timestamp = new Date(currentTime + RESET_INTERVAL_MS).toISOString();
                user.daily_ads_watched = 0;
                user.daily_spins = 0;
                await supabaseFetch('users', 'PATCH', { 
                    daily_ads_watched: 0,
                    daily_spins: 0,
                    reset_timestamp: user.reset_timestamp,
                    last_activity: dateNow,
                }, `?id=eq.${id}`);
            } else {
                 await supabaseFetch('users', 'PATCH', { 
                    last_activity: dateNow,
                 }, `?id=eq.${id}`);
            }
            
            const withdrawals = await supabaseFetch('withdrawals', 'GET', null, `?user_id=eq.${id}&order=requested_at.desc`);
            user.withdrawals = withdrawals;

            const referrals = await supabaseFetch('users', 'GET', null, `?referral_id=eq.${id}&count=exact&select=id`);
            const referral_count = referrals.length; 
            
            return sendSuccess(res, { 
                user: user, 
                referral_count: referral_count,
                message: shouldReset ? 'Limits reset and user data retrieved.' : 'User data retrieved.',
                is_new: false
            });
        }

    } catch (error) {
        console.error('GetUserData/Register failed:', error.message);
        return sendError(res, `Failed to fetch/register user data: ${error.message}`, 500);
    }
}

/**
 * 2) type: "requestActionId" - طلب معرف إجراء لمرة واحدة.
 */
async function handleRequestActionId(req, res, body) {
    const { user_id, action_type } = body;
    const id = parseInt(user_id);

    try {
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,is_banned,last_activity`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }
        
        if (isCooldownActive(user.last_activity)) {
            return sendError(res, 'Too many requests. Please wait a moment.', 429);
        }

        const actionId = generateActionId(id, action_type);
        sendSuccess(res, { action_id: actionId });

    } catch (error) {
        console.error('RequestActionId failed:', error.message);
        sendError(res, `Failed to generate action ID: ${error.message}`, 500);
    }
}

/**
 * 3) type: "watchAd" - مشاهدة إعلان والمطالبة بالمكافأة.
 */
async function handleWatchAd(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    const dateNow = new Date().toISOString();

    try {
        if (!consumeActionId(action_id, id, 'watchAd')) {
            return sendError(res, 'Invalid or expired action ID. Please try again.', 401);
        }
        
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,referral_id,is_banned,daily_ads_watched,last_activity`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }
        
        if (isCooldownActive(user.last_activity)) {
            return sendError(res, 'Too many requests. Please wait a moment.', 429);
        }

        if (user.daily_ads_watched >= DAILY_MAX_ADS) {
            return sendError(res, 'Daily ad limit reached. Please wait for the reset.', 403, 'limit');
        }

        const reward = REWARD_PER_AD;
        const newBalance = user.balance + reward;
        const newAdsWatched = user.daily_ads_watched + 1;

        const updatePayload = { 
            balance: newBalance,
            daily_ads_watched: newAdsWatched,
            last_ad_watch: dateNow,
            last_activity: dateNow
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        if (user.referral_id) {
            const commissionPayload = {
                type: 'commission',
                referral_id: user.referral_id,
                user_id: id,
                amount: reward * REFERRAL_COMMISSION_RATE
            };
            // محاولة استدعاء نقطة النهاية الداخلية (Commission) بشكل غير متزامن
            fetch(`https://${req.headers.host}/api/index.js`, { 
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(commissionPayload)
            }).catch(e => console.error('Commission call failed:', e));
        }

        sendSuccess(res, { 
            new_balance: newBalance, 
            daily_ads_watched: newAdsWatched, 
            reward: reward 
        });

    } catch (error) {
        console.error('WatchAd failed:', error.message);
        sendError(res, `Failed to process ad claim: ${error.message}`, 500);
    }
}

/**
 * 4) type: "commission" - معالجة عمولة الإحالة (نداء داخلي).
 */
async function handleCommission(req, res, body) {
    const { referral_id, user_id, amount } = body;
    const refId = parseInt(referral_id);
    const userId = parseInt(user_id);
    const dateNow = new Date().toISOString();
    
    try {
        const referrers = await supabaseFetch('users', 'GET', null, `?id=eq.${refId}&select=id,balance,is_banned`);
        if (!Array.isArray(referrers) || referrers.length === 0) {
            return sendError(res, `Referrer ${refId} not found.`, 404);
        }
        const referrer = referrers[0];

        if (referrer.is_banned) {
            return sendError(res, `Referrer ${refId} is banned. Commission cancelled.`, 403);
        }

        const commissionAmount = Math.floor(amount); 
        const newBalance = referrer.balance + commissionAmount;

        await supabaseFetch('users', 'PATCH', { 
            balance: newBalance,
            last_activity: dateNow,
        }, `?id=eq.${refId}`);

        await supabaseFetch('transactions', 'POST', {
            user_id: refId,
            type: 'commission',
            amount: commissionAmount,
            related_user_id: userId,
            created_at: dateNow
        });

        sendSuccess(res, { message: `Commission of ${commissionAmount} successfully applied to user ${refId}.` });

    } catch (error) {
        console.error('Commission failed:', error.message);
        sendError(res, `Failed to process commission: ${error.message}`, 500);
    }
}


/**
 * 5) type: "preSpin" - التحضير للدوران (عجلة الحظ).
 */
async function handlePreSpin(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);

    try {
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,is_banned,daily_spins,last_activity`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }
        
        if (isCooldownActive(user.last_activity)) {
            return sendError(res, 'Too many requests. Please wait a moment.', 429);
        }

        if (user.daily_spins >= DAILY_MAX_SPINS) {
            return sendError(res, 'Daily spin limit reached. Please wait for the reset.', 403, 'limit');
        }

        const actionId = generateActionId(id, 'spinResult');
        
        sendSuccess(res, { action_id: actionId });

    } catch (error) {
        console.error('PreSpin failed:', error.message);
        sendError(res, `Failed to initiate spin: ${error.message}`, 500);
    }
}


/**
 * 6) type: "spinResult" - معالجة نتيجة الدوران وتطبيق المكافأة.
 */
async function handleSpinResult(req, res, body) {
    const { user_id, action_id, result_index } = body;
    const id = parseInt(user_id);
    const dateNow = new Date().toISOString();

    try {
        if (!consumeActionId(action_id, id, 'spinResult')) {
            return sendError(res, 'Invalid or expired spin action ID. Please try the spin again.', 401);
        }

        const sectorIndex = parseInt(result_index);
        if (isNaN(sectorIndex) || sectorIndex < 0 || sectorIndex >= SPIN_SECTORS.length) {
            return sendError(res, 'Invalid spin result index.', 400);
        }
        
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,daily_spins,last_activity`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }
        
        if (isCooldownActive(user.last_activity)) {
            return sendError(res, 'Too many requests. Please wait a moment.', 429);
        }
        
        if (user.daily_spins >= DAILY_MAX_SPINS) {
            return sendError(res, 'Daily spin limit reached. Please wait for the reset.', 403, 'limit');
        }

        const reward = SPIN_SECTORS[sectorIndex];
        const newBalance = user.balance + reward;
        const newSpins = user.daily_spins + 1;

        const updatePayload = { 
            balance: newBalance,
            daily_spins: newSpins,
            last_spin: dateNow,
            last_activity: dateNow
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        sendSuccess(res, { 
            new_balance: newBalance, 
            daily_spins: newSpins, 
            reward: reward 
        });

    } catch (error) {
        console.error('SpinResult failed:', error.message);
        sendError(res, `Failed to process spin result: ${error.message}`, 500);
    }
}

/**
 * 7) type: "getTasks" - جلب قائمة المهام.
 */
async function handleGetTasks(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);

    try {
        const tasks = await supabaseFetch('tasks', 'GET', null, 
            `?select=id,name,link,reward,max_users,current_users,type&is_active=eq.true`);

        const completedTasksRecords = await supabaseFetch('user_tasks', 'GET', null, 
            `?user_id=eq.${id}&select=task_id`);
        const completedTaskIds = new Set(completedTasksRecords.map(rec => rec.task_id));

        const tasksWithStatus = tasks.map(task => ({
            ...task,
            is_completed: completedTaskIds.has(task.id),
            is_limit_reached: task.current_users >= task.max_users
        }));

        sendSuccess(res, { tasks: tasksWithStatus });

    } catch (error) {
        console.error('GetTasks failed:', error.message);
        sendError(res, `Failed to retrieve tasks: ${error.message}`, 500);
    }
}

/**
 * 8) type: "completeTask" - إكمال مهمة والمطالبة بالمكافأة.
 */
async function handleCompleteTask(req, res, body) {
    const { user_id, task_id } = body;
    const id = parseInt(user_id);
    const taskId = parseInt(task_id);
    const dateNow = new Date().toISOString();

    if (isNaN(taskId)) {
        return sendError(res, 'Missing or invalid task_id.', 400);
    }

    try {
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,last_activity`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }
        
        if (isCooldownActive(user.last_activity)) {
            return sendError(res, 'Too many requests. Please wait a moment.', 429);
        }

        const userTaskRecord = await supabaseFetch('user_tasks', 'GET', null, `?user_id=eq.${id}&task_id=eq.${taskId}&select=id`);
        if (userTaskRecord.length > 0) {
            return sendError(res, 'Reward already claimed. Task is complete.', 409);
        }

        const tasks = await supabaseFetch('tasks', 'GET', null, `?id=eq.${taskId}&select=name,link,reward,max_users,current_users,type,is_active`);
        if (!Array.isArray(tasks) || tasks.length === 0 || !tasks[0].is_active) {
            return sendError(res, 'Task not found or is inactive.', 404);
        }
        const task = tasks[0];
        
        if (task.current_users >= task.max_users) {
            return sendError(res, 'Maximum user limit for this task has been reached.', 403);
        }

        let isVerified = true;
        if (task.type === 'join_channel') {
             const isMember = await checkChannelMembership(id, task.link);
             if (!isMember) {
                 isVerified = false;
             }
        }
        
        if (!isVerified) {
            return sendError(res, 'Membership not verified. Please ensure you joined the channel and try again.', 400);
        }

        const newCurrentUsers = task.current_users + 1;
        const reward = task.reward;
        const newBalance = user.balance + reward;

        await supabaseFetch('tasks', 'PATCH', 
            { current_users: newCurrentUsers }, 
            `?id=eq.${taskId}`); 

        await supabaseFetch('user_tasks', 'POST', 
            { user_id: id, task_id: taskId, completed_at: dateNow });

        const updatePayload = { 
            balance: newBalance,
            last_activity: dateNow,
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        sendSuccess(res, { 
            new_balance: newBalance, 
            actual_reward: reward, 
            message: `Task "${task.name}" completed successfully.`,
        });

    } catch (error) {
        console.error('CompleteTask failed:', error.message);
        sendError(res, `Failed to complete task: ${error.message}`, 500);
    }
}


/**
 * 9) type: "withdraw" - معالجة طلب السحب.
 */
async function handleWithdraw(req, res, body) {
    const { user_id, binanceId, amount, action_id } = body;
    const id = parseInt(user_id);
    const dateNow = new Date().toISOString();
    const withdrawAmount = parseInt(amount);

    try {
        if (!consumeActionId(action_id, id, 'withdraw')) {
            return sendError(res, 'Invalid or expired withdrawal action ID. Please try again.', 401);
        }
        
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,last_activity`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }
        
        if (isCooldownActive(user.last_activity)) {
            return sendError(res, 'Too many requests. Please wait a moment.', 429);
        }

        if (isNaN(withdrawAmount) || withdrawAmount <= 0) {
            return sendError(res, 'Invalid withdrawal amount.', 400);
        }
        if (user.balance < withdrawAmount) {
            return sendError(res, 'Insufficient balance.', 403);
        }
        
        if (!binanceId || binanceId.length < 5 || binanceId.length > 50) {
            return sendError(res, 'Invalid Binance ID format.', 400);
        }

        const newBalance = user.balance - withdrawAmount;
        await supabaseFetch('users', 'PATCH', { 
            balance: newBalance,
            last_activity: dateNow,
        }, `?id=eq.${id}`);

        const [withdrawalRecord] = await supabaseFetch('withdrawals', 'POST', {
            user_id: id,
            binance_id: binanceId,
            amount: withdrawAmount,
            status: 'requested',
            requested_at: dateNow
        });

        sendSuccess(res, { 
            new_balance: newBalance, 
            withdrawal_record: withdrawalRecord 
        });

    } catch (error) {
        console.error('Withdraw failed:', error.message);
        sendError(res, `Failed to process withdrawal: ${error.message}`, 500);
    }
}


// ------------------------------------------------------------------
// دالة المعالج الرئيسية (نقطة الدخول - Entry Point)
// ------------------------------------------------------------------

/**
 * دالة المعالج الرئيسية لـ Vercel Serverless Function.
 */
module.exports = async (req, res) => {
  // 1. السماح بطلبات POST فقط
  if (req.method !== 'POST') {
    return sendError(res, 'Only POST requests are allowed.', 405);
  }

  let body;
  try {
      // 🛑🛑🛑 هذا هو الإصلاح الحاسم لخطأ TypeError في Vercel 🛑🛑🛑
      // قراءة جسم الطلب يدوياً من التدفق لضمان الحصول على بيانات JSON كاملة
      body = await new Promise((resolve, reject) => {
          let data = '';
          req.on('data', chunk => {
              data += chunk.toString();
          });
          req.on('end', () => {
              try {
                  if (data) {
                      resolve(JSON.parse(data));
                  } else {
                      resolve({}); 
                  }
              } catch (e) {
                  reject(new Error('Invalid JSON format in request body.'));
              }
          });
          req.on('error', reject);
      });
      // 🛑🛑🛑 نهاية الإصلاح 🛑🛑🛑

  } catch (error) {
      return sendError(res, error.message, 400);
  }
  
  if (!body || !body.type) {
    return sendError(res, 'Missing "type" field in the request body, or body is empty.', 400);
  }

  // ⬅️ التحقق الأمني لـ initData (يستثنى منه طلب العمولة الداخلي)
  if (body.type !== 'commission' && (!body.initData || !validateInitData(body.initData))) {
      return sendError(res, 'Invalid or expired initData. Security check failed.', 401);
  }

  if (!body.user_id && body.type !== 'commission') {
      return sendError(res, 'Missing user_id in the request body.', 400);
  }

  // توجيه الطلب بناءً على حقل 'type'
  switch (body.type) {
    case 'getUserData':
      await handleGetUserData(req, res, body);
      break;
    case 'requestActionId':
      await handleRequestActionId(req, res, body);
      break;
    case 'watchAd':
      await handleWatchAd(req, res, body);
      break;
    case 'commission':
      await handleCommission(req, res, body);
      break;
    case 'preSpin': 
      await handlePreSpin(req, res, body);
      break;
    case 'spinResult': 
      await handleSpinResult(req, res, body);
      break;
    case 'withdraw':
      await handleWithdraw(req, res, body);
      break;
    case 'completeTask': 
      await handleCompleteTask(req, res, body);
      break;
    case 'getTasks': 
      await handleGetTasks(req, res, body);
      break;
    default:
      sendError(res, `Unknown request type: ${body.type}`, 400);
  }
}

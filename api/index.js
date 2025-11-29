// /api/index.js (Final and Secure Version with Limit-Based Reset)

/**
 * SHIB Ads WebApp Backend API
 * Handles all POST requests from the Telegram Mini App frontend.
 * Uses the Supabase REST API for persistence.
 */
const crypto = require('crypto');

// Load environment variables for Supabase connection
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
// ⚠️ BOT_TOKEN must be set in Vercel environment variables
const BOT_TOKEN = process.env.BOT_TOKEN;

// ------------------------------------------------------------------
// Fully secured and defined server-side constants
// ------------------------------------------------------------------
const REWARD_PER_AD = 3;
const REFERRAL_COMMISSION_RATE = 0.05;
const DAILY_MAX_ADS = 100; // Max ads limit
const DAILY_MAX_SPINS = 15; // Max spins limit
const RESET_INTERVAL_MS = 6 * 60 * 60 * 1000; // ⬅️ 6 hours in milliseconds
const MIN_TIME_BETWEEN_ACTIONS_MS = 3000; // 3 seconds minimum time between watchAd/spin requests
const ACTION_ID_EXPIRY_MS = 60000; // 60 seconds for Action ID to be valid
const SPIN_SECTORS = [5, 10, 15, 20, 5, 10, 15, 20];

// ------------------------------------------------------------------
// NEW Task Constants - تم إزالتها: المهام أصبحت ديناميكية
// ------------------------------------------------------------------
// const TASK_REWARD = 50;
// const TELEGRAM_CHANNEL_USERNAME = '@botbababab'; 


/**
 * Helper function to randomly select a prize from the defined sectors and return its index.
 */
function calculateRandomSpinPrize() {
    const randomIndex = Math.floor(Math.random() * SPIN_SECTORS.length);
    const prize = SPIN_SECTORS[randomIndex];
    return { prize, prizeIndex: randomIndex };
}

// --- Helper Functions ---

function sendSuccess(res, data = {}) {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: true, data }));
}

function sendError(res, message, statusCode = 400) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: false, error: message }));
}

async function supabaseFetch(tableName, method, body = null, queryParams = '?select=*') {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    throw new Error('Supabase environment variables are not configured.');
  }

  const url = `${SUPABASE_URL}/rest/v1/${tableName}${queryParams}`;

  const headers = {
    'apikey': SUPABASE_ANON_KEY,
    'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
  };

  const options = {
    method,
    headers,
    body: body ? JSON.stringify(body) : null,
  };

  const response = await fetch(url, options);

  if (response.ok) {
      const responseText = await response.text();
      try {
          const jsonResponse = JSON.parse(responseText);
          // Return the array for SELECT, or a success object for POST/PATCH/DELETE
          return Array.isArray(jsonResponse) ? jsonResponse : { success: true, ...jsonResponse }; 
      } catch (e) {
          return { success: true };
      }
  }

  let data;
  try {
      data = await response.json();
  } catch (e) {
      const errorMsg = `Supabase error: ${response.status} ${response.statusText}`;
      throw new Error(errorMsg);
  }

  const errorMsg = data.message || `Supabase error: ${response.status} ${response.statusText}`;
  throw new Error(errorMsg);
}

/**
 * Checks if a user is a member (or creator/admin) of a specific Telegram channel.
 */
async function checkChannelMembership(userId, channelUsername) {
    if (!BOT_TOKEN) {
        console.error('BOT_TOKEN is not configured for membership check.');
        return false;
    }
    
    // The chat_id must be in the format @username or -100xxxxxxxxxx
    const chatId = channelUsername.startsWith('@') ? channelUsername : `@${channelUsername}`; 

    const url = `https://api.telegram.org/bot${BOT_TOKEN}/getChatMember?chat_id=${chatId}&user_id=${userId}`;
    
    try {
        const response = await fetch(url);
        if (!response.ok) {
            const errorData = await response.json();
            console.error('Telegram API error (getChatMember):', errorData.description || response.statusText);
            return false;
        }

        const data = await response.json();
        
        if (!data.ok) {
             console.error('Telegram API error (getChatMember - not ok):', data.description);
             return false;
        }

        const status = data.result.status;
        
        // Accepted statuses are 'member', 'administrator', 'creator'
        const isMember = ['member', 'administrator', 'creator'].includes(status);
        
        return isMember;

    } catch (error) {
        console.error('Network or parsing error during Telegram API call:', error.message);
        return false;
    }
}


/**
 * Limit-Based Reset Logic: Resets counters if the limit was reached AND the interval (6 hours) has passed since.
 */
async function resetDailyLimitsIfExpired(userId) {
    const now = Date.now();

    try {
        // 1. Fetch current limits and the time they were reached
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${userId}&select=ads_watched_today,spins_today,ads_limit_reached_at,spins_limit_reached_at`);
        if (!Array.isArray(users) || users.length === 0) {
            return;
        }

        const user = users[0];
        const updatePayload = {};

        // 2. Check Ads Limit Reset
        if (user.ads_limit_reached_at && user.ads_watched_today >= DAILY_MAX_ADS) {
            const adsLimitTime = new Date(user.ads_limit_reached_at).getTime();
            if (now - adsLimitTime > RESET_INTERVAL_MS) {
                // ⚠️ تم مرور 6 ساعات على الوصول للحد الأقصى، يتم إعادة التعيين
                updatePayload.ads_watched_today = 0;
                updatePayload.ads_limit_reached_at = null; // إزالة الوقت لانتهاء فترة القفل
                console.log(`Ads limit reset for user ${userId}.`);
            }
        }

        // 3. Check Spins Limit Reset
        if (user.spins_limit_reached_at && user.spins_today >= DAILY_MAX_SPINS) {
            const spinsLimitTime = new Date(user.spins_limit_reached_at).getTime();
            if (now - spinsLimitTime > RESET_INTERVAL_MS) {
                // ⚠️ تم مرور 6 ساعات على الوصول للحد الأقصى، يتم إعادة التعيين
                updatePayload.spins_today = 0;
                updatePayload.spins_limit_reached_at = null; // إزالة الوقت لانتهاء فترة القفل
                console.log(`Spins limit reset for user ${userId}.`);
            }
        }

        // 4. Perform the database update if any limits were reset
        if (Object.keys(updatePayload).length > 0) {
            await supabaseFetch('users', 'PATCH',
                updatePayload,
                `?id=eq.${userId}`);
        }
    } catch (error) {
        console.error(`Failed to check/reset daily limits for user ${userId}:`, error.message);
    }
}

/**
 * Rate Limiting Check for Ad/Spin Actions
 */
async function checkRateLimit(userId) {
    try {
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${userId}&select=last_activity`);
        if (!Array.isArray(users) || users.length === 0) {
            return { ok: true };
        }

        const user = users[0];
        // إذا كان last_activity غير موجود، يمكن اعتباره 0 لضمان السماح بالمرور
        const lastActivity = user.last_activity ? new Date(user.last_activity).getTime() : 0; 
        const now = Date.now();
        const timeElapsed = now - lastActivity;

        if (timeElapsed < MIN_TIME_BETWEEN_ACTIONS_MS) {
            const remainingTime = MIN_TIME_BETWEEN_ACTIONS_MS - timeElapsed;
            return {
                ok: false,
                message: `Rate limit exceeded. Please wait ${Math.ceil(remainingTime / 1000)} seconds before the next action.`,
                remainingTime: remainingTime
            };
        }
        // تحديث last_activity سيتم لاحقاً في دوال watchAd/spinResult
        return { ok: true };
    } catch (error) {
        console.error(`Rate limit check failed for user ${userId}:`, error.message);
        return { ok: true };
    }
}

// ------------------------------------------------------------------
// **initData Security Validation Function** (No change)
// ------------------------------------------------------------------
function validateInitData(initData) {
    if (!initData || !BOT_TOKEN) {
        console.warn('Security Check Failed: initData or BOT_TOKEN is missing.');
        return false;
    }

    const urlParams = new URLSearchParams(initData);
    const hash = urlParams.get('hash');
    urlParams.delete('hash');

    const dataCheckString = Array.from(urlParams.entries())
        .map(([key, value]) => `${key}=${value}`)
        .sort()
        .join('\n');

    const secretKey = crypto.createHmac('sha256', 'WebAppData')
        .update(BOT_TOKEN)
        .digest();

    const calculatedHash = crypto.createHmac('sha256', secretKey)
        .update(dataCheckString)
        .digest('hex');

    if (calculatedHash !== hash) {
        console.warn(`Security Check Failed: Hash mismatch.`);
        return false;
    }

    const authDateParam = urlParams.get('auth_date');
    if (!authDateParam) {
        console.warn('Security Check Failed: auth_date is missing.');
        return false;
    }

    const authDate = parseInt(authDateParam) * 1000;
    const currentTime = Date.now();
    const expirationTime = 1200 * 1000; // 20 minutes limit

    if (currentTime - authDate > expirationTime) {
        console.warn(`Security Check Failed: Data expired.`);
        return false;
    }

    return true;
}

// ------------------------------------------------------------------
// 🔑 Commission Helper Function (No change)
// ------------------------------------------------------------------
/**
 * Processes the commission for the referrer and updates their balance.
 */
async function processCommission(referrerId, refereeId, sourceReward) {
    // 1. Calculate commission
    const commissionAmount = sourceReward * REFERRAL_COMMISSION_RATE; 
    
    if (commissionAmount < 0.000001) { 
        console.log(`Commission too small (${commissionAmount}). Aborted for referee ${refereeId}.`);
        return { ok: false, error: 'Commission amount is effectively zero.' };
    }

    try {
        // 2. Fetch referrer's current balance and status
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${referrerId}&select=balance,is_banned`);
        if (!Array.isArray(users) || users.length === 0 || users[0].is_banned) {
             console.log(`Referrer ${referrerId} not found or banned. Commission aborted.`);
             return { ok: false, error: 'Referrer not found or banned, commission aborted.' };
        }
        
        // 3. Update balance: newBalance will now include the decimal commission
        const newBalance = users[0].balance + commissionAmount;
        
        // 4. Update referrer balance
        await supabaseFetch('users', 'PATCH', { balance: newBalance }, `?id=eq.${referrerId}`);

        // 5. Add record to commission_history
        await supabaseFetch('commission_history', 'POST', { referrer_id: referrerId, referee_id: refereeId, amount: commissionAmount, source_reward: sourceReward }, '?select=referrer_id');
        
        return { ok: true, new_referrer_balance: newBalance };
    } catch (error) {
        console.error('Commission failed:', error.message);
        return { ok: false, error: error.message };
    }
}

// ------------------------------------------------------------------
// 1) type: "getUserData" ⚠️ MODIFIED: Now fetches completed_task_ids
// ------------------------------------------------------------------
async function handleGetUserData(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);

    try {
        // 1. Fetch user data (balance, counts, referrer_id, etc.)
        // ⚠️ MODIFICATION: removed task_completed, will fetch dynamic tasks completed list
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,ads_watched_today,spins_today,ref_by,is_banned,last_activity`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const userData = users[0];

        // 2. Run reset logic (if needed)
        await resetDailyLimitsIfExpired(id);

        // 3. Fetch completed tasks IDs ⚠️ NEW
        // يجب أن يتطابق اسم الجدول مع إعداداتك (يفترض user_tasks_completed)
        const completedTasks = await supabaseFetch('user_tasks_completed', 'GET', null, `?user_id=eq.${id}&select=task_id`);
        const completedTaskIds = completedTasks.map(t => t.task_id);
        
        // 4. Banned Check
        if (userData.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 5. Fetch referrals count
        const referrals = await supabaseFetch('users', 'GET', null, `?ref_by=eq.${id}&select=id`);
        const referralsCount = referrals ? referrals.length : 0;
        
        // 6. Fetch withdrawal history
        const withdrawalHistory = await supabaseFetch('withdrawal_requests', 'GET', null, `?user_id=eq.${id}&select=amount,status,created_at&order=created_at.desc.limit(5)`);
        
        // 7. Success
        // Update last_activity before sending success (for rate limit)
        await supabaseFetch('users', 'PATCH', { last_activity: new Date().toISOString() }, `?id=eq.${id}&select=id`);

        // ⚠️ MODIFICATION: return completed_task_ids instead of task_completed
        sendSuccess(res, { 
            ...userData, 
            referrals_count: referralsCount, 
            withdrawal_history: withdrawalHistory, 
            completed_task_ids: completedTaskIds // ⬅️ قائمة المهام المنجزة
        });

    } catch (error) {
        console.error('GetUserData failed:', error.message);
        sendError(res, `Failed to retrieve user data: ${error.message}`, 500);
    }
}

// ------------------------------------------------------------------
// 2) type: "register" (No change)
// ------------------------------------------------------------------
async function handleRegister(req, res, body) {
    const { user_id, ref_by } = body;
    const id = parseInt(user_id);
    try {
        // 1. Check if user exists
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,is_banned`);
        if (!Array.isArray(users) || users.length === 0) {
            // 2. User does not exist, create new user
            const newUser = { 
                id, 
                balance: 0, 
                ads_watched_today: 0, 
                spins_today: 0, 
                ref_by: ref_by ? parseInt(ref_by) : null, 
                last_activity: new Date().toISOString(), 
                is_banned: false, 
                // task_completed: false, // ⚠️ REMOVED
                ads_limit_reached_at: null,
                spins_limit_reached_at: null,
            };
            const createdUser = await supabaseFetch('users', 'POST', newUser, '?select=*');

            // 3. Success
            return sendSuccess(res, { message: 'User registered.', user: createdUser });
        }
        
        // 4. User exists
        if (users[0].is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        sendSuccess(res, { message: 'User already registered.' });
        
    } catch (error) {
        console.error('Register failed:', error.message);
        sendError(res, `Failed to register user: ${error.message}`, 500);
    }
}

// ------------------------------------------------------------------
// 3) type: "watchAd" (No change)
// ------------------------------------------------------------------
async function handleWatchAd(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    const reward = REWARD_PER_AD;

    if (!action_id) {
        return sendError(res, 'Missing Action ID.', 400);
    }

    try {
        // 1. Validate Action ID (prevent replay attack)
        const actions = await supabaseFetch('action_tokens', 'GET', null, `?id=eq.${action_id}&select=id,user_id,action_type,created_at`);
        if (!Array.isArray(actions) || actions.length === 0 || actions[0].user_id !== id || actions[0].action_type !== 'watchAd') {
            return sendError(res, 'Invalid or expired Action ID for this action type.', 403);
        }
        
        const actionTime = new Date(actions[0].created_at).getTime();
        if (Date.now() - actionTime > ACTION_ID_EXPIRY_MS) {
            await supabaseFetch('action_tokens', 'DELETE', null, `?id=eq.${action_id}`);
            return sendError(res, 'Action ID expired.', 403);
        }
        
        // 2. Delete Action ID after validation (single use only)
        await supabaseFetch('action_tokens', 'DELETE', null, `?id=eq.${action_id}`);

        // 3. Fetch user and check limits
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,ads_watched_today,ref_by,is_banned`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        const referrerId = user.ref_by;

        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        if (user.ads_watched_today >= DAILY_MAX) {
            return sendError(res, 'Daily ad limit reached.', 403);
        }

        // 4. Perform update
        const newWatchedCount = user.ads_watched_today + 1;
        const newBalance = user.balance + reward;
        
        const updatePayload = { 
            balance: newBalance,
            ads_watched_today: newWatchedCount,
            last_activity: new Date().toISOString()
        };

        // If the limit is reached exactly, set the limit reached timestamp
        if (newWatchedCount === DAILY_MAX) {
            updatePayload.ads_limit_reached_at = new Date().toISOString();
        }
        
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 5. Commission Call
        if (referrerId) {
            processCommission(referrerId, id, reward).catch(e => {
                console.error(`WatchAd Commission failed silently for referrer ${referrerId}:`, e.message);
            });
        }

        // 6. Success
        sendSuccess(res, { new_balance: newBalance, reward: reward, ads_watched_today: newWatchedCount });

    } catch (error) {
        console.error('WatchAd failed:', error.message);
        sendError(res, `Failed to process ad watch: ${error.message}`, 500);
    }
}


// ------------------------------------------------------------------
// 4) type: "commission" (No change)
// ------------------------------------------------------------------
async function handleCommission(req, res, body) {
    // This is primarily for testing or backend-to-backend communication, 
    // though the frontend initiates it indirectly via watchAd/spinResult/claimTask.
    // However, if the client sends a 'commission' request directly, it should be rejected 
    // unless a proper server-to-server auth mechanism is used. 
    // Since this is a public API, we'll keep the implementation simple.
    // The commission logic is now inside the reward functions (watchAd, spinResult, claimTask)
    return sendError(res, 'Direct commission claims are not allowed.', 403);
}


// ------------------------------------------------------------------
// 5) type: "preSpin" (No change)
// ------------------------------------------------------------------
async function handlePreSpin(req, res, body) {
    const { user_id, action_type } = body;
    const id = parseInt(user_id);
    
    // 1. Rate Limit Check
    const rateLimitResult = await checkRateLimit(id);
    if (!rateLimitResult.ok) {
        return sendError(res, rateLimitResult.message, 429);
    }

    try {
        // 2. Generate a single-use action token
        const actionId = crypto.randomBytes(16).toString('hex');
        const token = {
            id: actionId,
            user_id: id,
            action_type: action_type, // 'watchAd', 'spin', or 'withdraw'
            created_at: new Date().toISOString()
        };

        await supabaseFetch('action_tokens', 'POST', token, '?select=id');
        
        // 3. If action is 'spin', pre-calculate the result
        if (action_type === 'spin') {
             const { prize, prizeIndex } = calculateRandomSpinPrize();
             
             // 4. Update the token with the calculated prize (server-side decision)
             await supabaseFetch('action_tokens', 'PATCH', { payload: { prize, prizeIndex } }, `?id=eq.${actionId}`);

             return sendSuccess(res, { action_id: actionId, prize, prizeIndex });
        }
        
        // 4. Success (for general actions like watchAd/withdraw)
        sendSuccess(res, { action_id: actionId });

    } catch (error) {
        console.error('PreSpin failed:', error.message);
        sendError(res, `Failed to prepare action: ${error.message}`, 500);
    }
}

// ------------------------------------------------------------------
// 6) type: "spinResult" (No change)
// ------------------------------------------------------------------
async function handleSpinResult(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    
    if (!action_id) {
        return sendError(res, 'Missing Action ID.', 400);
    }

    try {
        // 1. Validate Action ID and retrieve prize from the token
        const actions = await supabaseFetch('action_tokens', 'GET', null, `?id=eq.${action_id}&select=id,user_id,action_type,created_at,payload`);
        if (!Array.isArray(actions) || actions.length === 0 || actions[0].user_id !== id || actions[0].action_type !== 'spin' || !actions[0].payload) {
            return sendError(res, 'Invalid or expired Action ID for spin.', 403);
        }
        
        const actionTime = new Date(actions[0].created_at).getTime();
        if (Date.now() - actionTime > ACTION_ID_EXPIRY_MS) {
            await supabaseFetch('action_tokens', 'DELETE', null, `?id=eq.${action_id}`);
            return sendError(res, 'Action ID expired.', 403);
        }
        
        // The prize amount is determined by the server and stored in the token
        const prize = actions[0].payload.prize; 
        
        // 2. Delete Action ID after validation (single use only)
        await supabaseFetch('action_tokens', 'DELETE', null, `?id=eq.${action_id}`);

        // 3. Fetch user and check limits
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,spins_today,ref_by,is_banned`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        const referrerId = user.ref_by;

        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        if (user.spins_today >= DAILY_MAX_SPINS) {
            return sendError(res, 'Daily spin limit reached.', 403);
        }

        // 4. Perform update
        const newSpinCount = user.spins_today + 1;
        const newBalance = user.balance + prize;
        
        const updatePayload = { 
            balance: newBalance,
            spins_today: newSpinCount,
            last_activity: new Date().toISOString()
        };
        
        // If the limit is reached exactly, set the limit reached timestamp
        if (newSpinCount === DAILY_MAX_SPINS) {
            updatePayload.spins_limit_reached_at = new Date().toISOString();
        }
        
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 5. Commission Call
        if (referrerId) {
            processCommission(referrerId, id, prize).catch(e => {
                console.error(`SpinResult Commission failed silently for referrer ${referrerId}:`, e.message);
            });
        }

        // 6. Success
        sendSuccess(res, { new_balance: newBalance, prize: prize, spins_today: newSpinCount });

    } catch (error) {
        console.error('SpinResult failed:', error.message);
        sendError(res, `Failed to process spin result: ${error.message}`, 500);
    }
}


// ------------------------------------------------------------------
// 7) type: "withdraw" (No change)
// ------------------------------------------------------------------
async function handleWithdraw(req, res, body) {
    const { user_id, binanceId, amount, action_id } = body;
    const id = parseInt(user_id);
    const floatAmount = parseFloat(amount);

    if (!binanceId || !floatAmount || floatAmount <= 0) {
        return sendError(res, 'Invalid withdrawal details.', 400);
    }
    if (!action_id) {
        return sendError(res, 'Missing Action ID.', 400);
    }

    try {
        // 1. Validate Action ID (prevent replay attack)
        const actions = await supabaseFetch('action_tokens', 'GET', null, `?id=eq.${action_id}&select=id,user_id,action_type,created_at`);
        if (!Array.isArray(actions) || actions.length === 0 || actions[0].user_id !== id || actions[0].action_type !== 'withdraw') {
            return sendError(res, 'Invalid or expired Action ID for withdrawal.', 403);
        }
        
        const actionTime = new Date(actions[0].created_at).getTime();
        if (Date.now() - actionTime > ACTION_ID_EXPIRY_MS) {
            await supabaseFetch('action_tokens', 'DELETE', null, `?id=eq.${action_id}`);
            return sendError(res, 'Action ID expired.', 403);
        }
        
        // 2. Delete Action ID after validation (single use only)
        await supabaseFetch('action_tokens', 'DELETE', null, `?id=eq.${action_id}`);


        // 3. Fetch user and check balance
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,is_banned`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];

        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        if (user.balance < floatAmount) {
            return sendError(res, `Insufficient balance. Available: ${user.balance}. Requested: ${floatAmount}`, 403);
        }
        
        // 4. Deduct amount and update balance
        const newBalance = user.balance - floatAmount;
        await supabaseFetch('users', 'PATCH', { balance: newBalance, last_activity: new Date().toISOString() }, `?id=eq.${id}`);

        // 5. Create withdrawal request record
        await supabaseFetch('withdrawal_requests', 'POST', { 
            user_id: id, 
            amount: floatAmount, 
            target_id: binanceId, 
            status: 'pending' 
        }, '?select=user_id');

        // 6. Success
        sendSuccess(res, { new_balance: newBalance, message: 'Withdrawal request submitted successfully.' });

    } catch (error) {
        console.error('Withdrawal failed:', error.message);
        sendError(res, `Failed to process withdrawal: ${error.message}`, 500);
    }
}

// ------------------------------------------------------------------
// 8) type: "getTasks" ⚠️ NEW: Fetches active dynamic tasks
// ------------------------------------------------------------------
/**
 * 8) type: "getTasks"
 * Fetches all active tasks and returns them.
 */
async function handleGetTasks(req, res, body) {
    try {
        // 1. Fetch all active tasks where current_users is less than max_users
        const tasks = await supabaseFetch('dynamic_tasks', 'GET', null, `?select=id,name,link,reward,max_users,current_users,channel_username&current_users=lt.max_users&order=id.asc`); 

        sendSuccess(res, { tasks });
    } catch (error) {
        console.error('GetTasks failed:', error.message);
        sendError(res, `Failed to retrieve tasks: ${error.message}`, 500);
    }
}

// ------------------------------------------------------------------
// 9) type: "claimTask" ⚠️ NEW: Replaces handleCompleteTask
// ------------------------------------------------------------------
/**
 * 9) type: "claimTask" (Replaces completeTask)
 * Claims the reward for a specific dynamic task.
 */
async function handleClaimTask(req, res, body) {
    const { user_id, task_id, channel_username } = body;
    const id = parseInt(user_id);
    const taskId = parseInt(task_id);

    if (!taskId || !channel_username) {
        return sendError(res, 'Missing task_id or channel_username.', 400);
    }

    // 1. Rate Limit Check 
    const rateLimitResult = await checkRateLimit(id);
    if (!rateLimitResult.ok) {
        return sendError(res, rateLimitResult.message, 429);
    }
    
    try {
        // 2. Fetch User, Task details, and check if already completed or banned
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,ref_by`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        const referrerId = user.ref_by;
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // Check if user already completed this task
        const completedCheck = await supabaseFetch('user_tasks_completed', 'GET', null, `?user_id=eq.${id}&task_id=eq.${taskId}&select=id`);
        if (Array.isArray(completedCheck) && completedCheck.length > 0) {
            return sendError(res, 'Task already completed by user.', 409);
        }
        
        // Fetch task details and check limits
        const task = await supabaseFetch('dynamic_tasks', 'GET', null, `?id=eq.${taskId}&select=reward,max_users,current_users,channel_username`);
        if (!Array.isArray(task) || task.length === 0) {
            return sendError(res, 'Task not found or is inactive.', 404);
        }
        const taskData = task[0];
        
        // Ensure channel_username matches the one stored on the server for the task (security check)
        if (taskData.channel_username !== channel_username) {
             return sendError(res, 'Task data mismatch or invalid channel username provided.', 400);
        }

        if (taskData.current_users >= taskData.max_users) {
            return sendError(res, 'Maximum user limit reached for this task.', 403);
        }

        // 3. Check Channel Membership
        // نستخدم channel_username القادم من الطلب لمرونة أكبر ولكن يجب التأكد من أنه مطابق لما هو مخزن في قاعدة البيانات
        const isMember = await checkChannelMembership(id, channel_username); 
        if (!isMember) {
            return sendError(res, 'User has not joined the required channel.', 400);
        }

        const reward = taskData.reward;
        const newBalance = user.balance + reward;
        
        // 4. Record Completion and Update Counts
        
        // a. Add record to user_tasks_completed
        await supabaseFetch('user_tasks_completed', 'POST', { user_id: id, task_id: taskId }, '?select=id');
        
        // b. Increment current_users count in dynamic_tasks
        await supabaseFetch('dynamic_tasks', 'PATCH', { current_users: taskData.current_users + 1 }, `?id=eq.${taskId}`);
        
        // c. Update User Balance and last_activity
        const updatePayload = { 
            balance: newBalance,
            last_activity: new Date().toISOString() // Update for Rate Limit
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 5. Commission Call
        if (referrerId) {
            processCommission(referrerId, id, reward).catch(e => {
                console.error(`ClaimTask Commission failed silently for referrer ${referrerId}:`, e.message);
            });
        }

        // 6. Success
        sendSuccess(res, { new_balance: newBalance, actual_reward: reward, message: 'Task completed successfully.', claimed_task_id: taskId });

    } catch (error) {
        console.error('ClaimTask failed:', error.message);
        sendError(res, `Failed to claim task reward: ${error.message}`, 500);
    }
}


/**
 * Main handler function for all incoming requests.
 */
export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return sendError(res, 'Only POST requests are allowed.', 405);
  }

  let body;
  try {
    body = await new Promise((resolve, reject) => {
      let data = '';
      req.on('data', chunk => {
        data += chunk.toString();
      });
      req.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error('Invalid JSON payload.'));
        }
      });
      req.on('error', reject);
    });
  } catch (error) {
    return sendError(res, error.message, 400);
  }

  if (!body || !body.type) {
    return sendError(res, 'Missing "type" field in the request body.', 400);
  }

  // ⬅️ initData Security Check
  if (body.type !== 'commission' && (!body.initData || !validateInitData(body.initData))) {
      return sendError(res, 'Invalid or expired initData. Security check failed.', 401);
  }

  if (!body.user_id && body.type !== 'commission') {
      return sendError(res, 'Missing user_id in the request body.', 400);
  }

  // Route the request based on the 'type' field
  switch (body.type) {
    case 'getUserData':
      await handleGetUserData(req, res, body);
      break;
    case 'register':
      await handleRegister(req, res, body);
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
    // ⚠️ NEW: Fetch dynamic tasks
    case 'getTasks':
        await handleGetTasks(req, res, body);
        break;
    // ⚠️ MODIFIED: Replaced 'completeTask' with 'claimTask' for dynamic tasks
    case 'claimTask': 
      await handleClaimTask(req, res, body);
      break;
    default:
      sendError(res, 'Invalid request type.', 400);
      break;
  }
}
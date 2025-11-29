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
const SPIN_SECTORS = [5, 10, 15, 20, 5];

// ------------------------------------------------------------------
// NEW Task Constants
// ------------------------------------------------------------------
const TASK_REWARD = 200; // ⬅️ This is now overridden by dynamic tasks but kept for backward compatibility if needed

// ------------------------------------------------------------------
// Security and Utility Helpers
// ------------------------------------------------------------------

/**
 * Sends a successful JSON response.
 */
function sendSuccess(res, data, status = 200) {
    res.status(status).json({ ok: true, data });
}

/**
 * Sends an error JSON response.
 */
function sendError(res, message, status = 400, errorType = 'error', errorTitle = 'Operation Failed') {
    res.status(status).json({ 
        ok: false, 
        error: message, 
        errorType: errorType,
        errorTitle: errorTitle,
        cleanMessage: message.split(':')[0] // Simple way to clean up database errors for display
    });
}

/**
 * Validates Telegram Mini App initData using the Bot Token.
 * @param {string} initData - The data string from the frontend.
 * @returns {boolean} True if data is valid, false otherwise.
 */
function validateInitData(initData) {
    const urlParams = new URLSearchParams(initData);
    const hash = urlParams.get('hash');
    const params = [];
    
    // Collect all parameters except 'hash' and sort them alphabetically
    for (const [key, value] of urlParams.entries()) {
        if (key !== 'hash') {
            params.push(`${key}=${value}`);
        }
    }
    const dataCheckString = params.sort().join('\n');
    
    // Hash the bot token to get the secret key
    const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    
    // Compute the hash of the data check string using the secret key
    const computedHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');
    
    // Compare the computed hash with the received hash
    return computedHash === hash;
}

/**
 * A generalized function to fetch data from Supabase using the REST API.
 * @param {string} table - The table name.
 * @param {string} method - HTTP method (GET, POST, PATCH).
 * @param {object | null} body - The request body for POST/PATCH.
 * @param {string} urlParams - Query parameters (e.g., '?id=eq.123&select=balance').
 * @returns {Promise<any>} The parsed JSON response data.
 */
async function supabaseFetch(table, method, body = null, urlParams = '') {
    const url = `${SUPABASE_URL}/rest/v1/${table}${urlParams}`;
    
    const headers = {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=representation' // Ensures the server returns the updated/inserted object
    };

    const config = {
        method: method,
        headers: headers,
    };

    if (body) {
        config.body = JSON.stringify(body);
    }
    
    const response = await fetch(url, config);
    
    // Supabase returns 204 No Content for successful PATCH/DELETE if 'return=minimal' is preferred,
    // but we use 'return=representation' so we expect content for POST/PATCH.
    if (response.status === 204 && method === 'PATCH') {
        return []; // Return an empty array for successful update with no content
    }

    if (!response.ok) {
        // Try to read error message from response body
        let errorBody = await response.text();
        try {
            errorBody = JSON.parse(errorBody);
        } catch (e) {
            // ignore JSON parse error
        }
        const errorDetail = typeof errorBody === 'object' && errorBody.message ? errorBody.message : response.statusText;
        throw new Error(`Supabase error (${response.status} ${table}): ${errorDetail}`);
    }

    // Always return the JSON body if it exists (e.g., for GET/POST)
    return response.json();
}


/**
 * Helper function to check if a user is a member of a Telegram channel.
 * Uses the Telegram Bot API: getChatMember.
 * @param {number} userId - The user's Telegram ID.
 * @param {string} channelUsername - The channel username (e.g., '@channelname' or full link).
 * @returns {Promise<boolean>} True if the user is a member, false otherwise.
 */
async function checkChannelMembership(userId, channelUsername) {
    // Clean the username: remove link and '@'
    let chatUsername = channelUsername.replace('https://t.me/', '');
    chatUsername = chatUsername.startsWith('@') ? chatUsername : `@${chatUsername}`;
    
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`;

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: chatUsername, // Channel username or ID
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
            // The user is a member if status is 'member', 'creator', or 'administrator'
            return ['member', 'administrator', 'creator'].includes(status);
        } else {
            // Handle cases where the bot cannot access the channel or user ID is invalid
            console.error('Telegram API responded with error:', data.description);
            // Assuming failure means not a member for safety
            return false; 
        }

    } catch (error) {
        console.error('Network error during checkChannelMembership:', error.message);
        return false;
    }
}


// ------------------------------------------------------------------
// Action ID and Anti-Spam Helpers
// ------------------------------------------------------------------

/** Global map to store valid, one-time action IDs: { id: { user_id: number, type: string, timestamp: number } } */
const actionIdStore = new Map();

/**
 * Generates a unique action ID and stores it temporarily.
 * @param {number} user_id - The user's ID.
 * @param {string} type - The action type (e.g., 'watchAd', 'spin', 'withdraw').
 * @returns {string} The generated action ID.
 */
function generateActionId(user_id, type) {
    const id = crypto.randomBytes(16).toString('hex');
    actionIdStore.set(id, {
        user_id: user_id,
        type: type,
        timestamp: Date.now()
    });
    // Cleanup old IDs periodically (optional, relies on serverless env specifics)
    setTimeout(cleanupActionIds, ACTION_ID_EXPIRY_MS + 5000); 
    return id;
}

/**
 * Validates an action ID and consumes it (removes it from the store).
 * @param {string} id - The received action ID.
 * @param {number} user_id - The user's ID to verify against the stored ID.
 * @param {string} type - The action type to verify against the stored ID.
 * @returns {boolean} True if valid and consumed, false otherwise.
 */
function consumeActionId(id, user_id, type) {
    const record = actionIdStore.get(id);

    if (!record) {
        return false; // ID not found or already consumed/expired
    }

    // Verify user ID and type
    if (record.user_id !== user_id || record.type !== type) {
        return false;
    }

    // Check for expiry
    if (Date.now() - record.timestamp > ACTION_ID_EXPIRY_MS) {
        actionIdStore.delete(id); // Expired
        return false;
    }

    // Valid: consume and remove
    actionIdStore.delete(id);
    return true;
}

/**
 * Simple cleanup of expired action IDs.
 */
function cleanupActionIds() {
    const now = Date.now();
    for (const [id, record] of actionIdStore.entries()) {
        if (now - record.timestamp > ACTION_ID_EXPIRY_MS) {
            actionIdStore.delete(id);
        }
    }
}

/**
 * Checks if enough time has passed since the last action.
 * @param {number} lastActivity - Timestamp of the last user activity.
 * @returns {boolean} True if cooldown is active, false otherwise.
 */
function isCooldownActive(lastActivity) {
    if (!lastActivity) return false;
    const lastActivityTime = new Date(lastActivity).getTime();
    return (Date.now() - lastActivityTime) < MIN_TIME_BETWEEN_ACTIONS_MS;
}

// ------------------------------------------------------------------
// Request Handlers
// ------------------------------------------------------------------

/**
 * 1) type: "getUserData"
 * Retrieves user data, registers if not found.
 */
async function handleGetUserData(req, res, body) {
    const { user_id, telegram_username, first_name, photo_url } = body;
    const id = parseInt(user_id);
    const dateNow = new Date().toISOString();

    try {
        // 1. Try to find the user
        let users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=*`);
        let user = users.length > 0 ? users[0] : null;

        if (!user) {
            // 2. User not found, proceed to register
            const referralId = body.referral_id ? parseInt(body.referral_id) : null;
            
            if (referralId && referralId === id) {
                // User cannot refer themselves
                sendError(res, 'Invalid referral ID: Cannot self-refer.', 400);
                return;
            }

            // 3. Check if referral ID is valid (exists)
            let referrer = null;
            if (referralId) {
                const referrers = await supabaseFetch('users', 'GET', null, `?id=eq.${referralId}&select=id`);
                referrer = referrers.length > 0 ? referrers[0] : null;

                if (!referrer) {
                    // Invalid referrer, clear referral ID
                    console.warn(`Referral ID ${referralId} is invalid for user ${id}.`);
                }
            }

            // 4. Register the new user
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

            // 5. Success
            return sendSuccess(res, { 
                user: user, 
                message: 'User registered successfully.',
                is_new: true
            });

        } else {
            // 3. User found, apply reset check (based on 6-hour interval)
            const currentTime = Date.now();
            const resetTime = new Date(user.reset_timestamp).getTime();
            let shouldReset = false;

            if (currentTime >= resetTime) {
                shouldReset = true;
                // Calculate the next reset time (current time + interval)
                user.reset_timestamp = new Date(currentTime + RESET_INTERVAL_MS).toISOString();
                user.daily_ads_watched = 0;
                user.daily_spins = 0;
                // Update the user record with the reset values
                await supabaseFetch('users', 'PATCH', { 
                    daily_ads_watched: 0,
                    daily_spins: 0,
                    reset_timestamp: user.reset_timestamp,
                    last_activity: dateNow,
                }, `?id=eq.${id}`);
            } else {
                 // Update activity time only
                 await supabaseFetch('users', 'PATCH', { 
                    last_activity: dateNow,
                 }, `?id=eq.${id}`);
            }
            
            // 4. Get withdrawal history
            const withdrawals = await supabaseFetch('withdrawals', 'GET', null, `?user_id=eq.${id}&order=requested_at.desc`);
            user.withdrawals = withdrawals;

            // 5. Get referrals count
            const referrals = await supabaseFetch('users', 'GET', null, `?referral_id=eq.${id}&count=exact&select=id`);
            const referral_count = referrals.length; // Count property is usually in the headers, but this works with 'select=id' if count is not available in headers
            
            // 6. Success
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
 * 2) type: "requestActionId"
 * Generates and returns a one-time Action ID for an upcoming action.
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
 * 3) type: "watchAd"
 * Handles the logic for a user watching an ad and claiming the reward.
 */
async function handleWatchAd(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    const dateNow = new Date().toISOString();

    try {
        // 1. Action ID and Cooldown Check
        if (!consumeActionId(action_id, id, 'watchAd')) {
            return sendError(res, 'Invalid or expired action ID. Please try again.', 401);
        }
        
        // 2. Fetch user data (including referral_id, limits, and activity)
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

        // 3. Daily Limit Check
        if (user.daily_ads_watched >= DAILY_MAX_ADS) {
            return sendError(res, 'Daily ad limit reached. Please wait for the reset.', 403, 'limit');
        }

        // 4. Calculate new state
        const reward = REWARD_PER_AD;
        const newBalance = user.balance + reward;
        const newAdsWatched = user.daily_ads_watched + 1;

        // 5. Update user balance and counter
        const updatePayload = { 
            balance: newBalance,
            daily_ads_watched: newAdsWatched,
            last_ad_watch: dateNow,
            last_activity: dateNow
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 6. Referral Commission Check (Asynchronously/Fire-and-Forget)
        if (user.referral_id) {
            // Send commission request to the server, which handles its own security
            const commissionPayload = {
                type: 'commission',
                referral_id: user.referral_id,
                user_id: id,
                amount: reward * REFERRAL_COMMISSION_RATE
            };
            // Note: In a Vercel environment, calling the API endpoint itself can be tricky/expensive. 
            // A more robust solution involves direct DB updates or a separate queue/worker. 
            // For simplicity, we assume this internal call is possible and reliable.
            fetch(`https://${req.headers.host}/api/index.js`, { // Assumes standard Vercel setup
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(commissionPayload)
            }).catch(e => console.error('Commission call failed:', e));
        }

        // 7. Success
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
 * 4) type: "commission"
 * Handles the passive commission update for the referrer (Internal/System Call).
 */
async function handleCommission(req, res, body) {
    const { referral_id, user_id, amount } = body;
    const refId = parseInt(referral_id);
    const userId = parseInt(user_id);
    const dateNow = new Date().toISOString();
    
    // ⚠️ WARNING: This endpoint needs proper internal authentication in a real-world scenario.
    // For this simple example, we rely on it being called by the server itself (handleWatchAd).
    
    try {
        // 1. Fetch referrer's current balance
        const referrers = await supabaseFetch('users', 'GET', null, `?id=eq.${refId}&select=id,balance,is_banned`);
        if (!Array.isArray(referrers) || referrers.length === 0) {
            return sendError(res, `Referrer ${refId} not found.`, 404);
        }
        const referrer = referrers[0];

        if (referrer.is_banned) {
            // Do not give commission to banned users
            return sendError(res, `Referrer ${refId} is banned. Commission cancelled.`, 403);
        }

        // 2. Calculate new state
        const commissionAmount = Math.floor(amount); // Ensure commission is integer
        const newBalance = referrer.balance + commissionAmount;

        // 3. Update referrer balance
        await supabaseFetch('users', 'PATCH', { 
            balance: newBalance,
            last_activity: dateNow,
        }, `?id=eq.${refId}`);

        // 4. Record the transaction (Optional, but good practice)
        await supabaseFetch('transactions', 'POST', {
            user_id: refId,
            type: 'commission',
            amount: commissionAmount,
            related_user_id: userId,
            created_at: dateNow
        });

        // 5. Success
        sendSuccess(res, { message: `Commission of ${commissionAmount} successfully applied to user ${refId}.` });

    } catch (error) {
        console.error('Commission failed:', error.message);
        sendError(res, `Failed to process commission: ${error.message}`, 500);
    }
}


/**
 * 5) type: "preSpin"
 * Handles the start of the spin, checks limits, and provides the action ID.
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

        // Generate a new one-time action ID for the spin result
        const actionId = generateActionId(id, 'spinResult');
        
        // Success
        sendSuccess(res, { action_id: actionId });

    } catch (error) {
        console.error('PreSpin failed:', error.message);
        sendError(res, `Failed to initiate spin: ${error.message}`, 500);
    }
}


/**
 * 6) type: "spinResult"
 * Handles the final result of the spin, calculates reward, and updates balance.
 */
async function handleSpinResult(req, res, body) {
    const { user_id, action_id, result_index } = body;
    const id = parseInt(user_id);
    const dateNow = new Date().toISOString();

    try {
        // 1. Action ID Check
        if (!consumeActionId(action_id, id, 'spinResult')) {
            return sendError(res, 'Invalid or expired spin action ID. Please try the spin again.', 401);
        }

        // 2. Input Validation (Ensure result_index is safe and valid)
        const sectorIndex = parseInt(result_index);
        if (isNaN(sectorIndex) || sectorIndex < 0 || sectorIndex >= SPIN_SECTORS.length) {
            return sendError(res, 'Invalid spin result index.', 400);
        }
        
        // 3. Fetch user data (limits and activity)
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
        
        // Re-check limit to prevent race conditions or missed 'preSpin' limit checks
        if (user.daily_spins >= DAILY_MAX_SPINS) {
            return sendError(res, 'Daily spin limit reached. Please wait for the reset.', 403, 'limit');
        }

        // 4. Calculate reward and new state
        const reward = SPIN_SECTORS[sectorIndex];
        const newBalance = user.balance + reward;
        const newSpins = user.daily_spins + 1;

        // 5. Update user balance and counter
        const updatePayload = { 
            balance: newBalance,
            daily_spins: newSpins,
            last_spin: dateNow,
            last_activity: dateNow
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 6. Success
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
 * 7) type: "getTasks"
 * Fetches all active tasks and the user's completion status.
 */
async function handleGetTasks(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);

    try {
        // 1. Fetch active tasks with their limits and counters
        const tasks = await supabaseFetch('tasks', 'GET', null, 
            `?select=id,name,link,reward,max_users,current_users,type&is_active=eq.true`);

        // 2. Fetch user's completed tasks from the new user_tasks table
        const completedTasksRecords = await supabaseFetch('user_tasks', 'GET', null, 
            `?user_id=eq.${id}&select=task_id`);
        const completedTaskIds = new Set(completedTasksRecords.map(rec => rec.task_id));

        // 3. Combine tasks with user status and limit status
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
 * 8) type: "completeTask" (MODIFIED to accept task_id)
 * Handles the completion and reward claiming for a specific task.
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
        // 1. Basic checks (user banned, user exists)
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

        // 2. Check if the user has already completed this task
        const userTaskRecord = await supabaseFetch('user_tasks', 'GET', null, `?user_id=eq.${id}&task_id=eq.${taskId}&select=id`);
        if (userTaskRecord.length > 0) {
            return sendError(res, 'Reward already claimed. Task is complete.', 409);
        }

        // 3. Fetch task details and check global limits
        const tasks = await supabaseFetch('tasks', 'GET', null, `?id=eq.${taskId}&select=name,link,reward,max_users,current_users,type,is_active`);
        if (!Array.isArray(tasks) || tasks.length === 0 || !tasks[0].is_active) {
            return sendError(res, 'Task not found or is inactive.', 404);
        }
        const task = tasks[0];
        
        if (task.current_users >= task.max_users) {
            return sendError(res, 'Maximum user limit for this task has been reached.', 403);
        }

        // 4. Perform Task Verification (if task type is 'join_channel')
        let isVerified = true;
        if (task.type === 'join_channel') {
             // 'link' is the channel username (e.g., @channelname or https://t.me/channelname)
             const isMember = await checkChannelMembership(id, task.link);
             if (!isMember) {
                 isVerified = false;
             }
        }
        
        if (!isVerified) {
            return sendError(res, 'Membership not verified. Please ensure you joined the channel and try again.', 400);
        }

        // 5. Update: Increment task counter, record completion, update user balance
        const newCurrentUsers = task.current_users + 1;
        const reward = task.reward;
        const newBalance = user.balance + reward;

        // A. Increment task counter in 'tasks' table
        await supabaseFetch('tasks', 'PATCH', 
            { current_users: newCurrentUsers }, 
            `?id=eq.${taskId}`); 

        // B. Record task completion in 'user_tasks' table
        await supabaseFetch('user_tasks', 'POST', 
            { user_id: id, task_id: taskId, completed_at: dateNow });

        // C. Update user balance and activity in 'users' table
        const updatePayload = { 
            balance: newBalance,
            last_activity: dateNow,
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 6. Success
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
 * 9) type: "requestActionId"
 * Generates and returns a one-time Action ID for an upcoming action.
 */
async function handleWithdraw(req, res, body) {
    const { user_id, binanceId, amount, action_id } = body;
    const id = parseInt(user_id);
    const dateNow = new Date().toISOString();
    const withdrawAmount = parseInt(amount);

    try {
        // 1. Action ID Check
        if (!consumeActionId(action_id, id, 'withdraw')) {
            return sendError(res, 'Invalid or expired withdrawal action ID. Please try again.', 401);
        }
        
        // 2. Cooldown check (important for this critical action)
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

        // 3. Validation: Amount check
        if (isNaN(withdrawAmount) || withdrawAmount <= 0) {
            return sendError(res, 'Invalid withdrawal amount.', 400);
        }
        if (user.balance < withdrawAmount) {
            return sendError(res, 'Insufficient balance.', 403);
        }
        
        // 4. Validation: Binance ID format
        if (!binanceId || binanceId.length < 5 || binanceId.length > 50) {
            return sendError(res, 'Invalid Binance ID format.', 400);
        }

        // 5. Update user balance
        const newBalance = user.balance - withdrawAmount;
        await supabaseFetch('users', 'PATCH', { 
            balance: newBalance,
            last_activity: dateNow,
        }, `?id=eq.${id}`);

        // 6. Record the withdrawal request
        const [withdrawalRecord] = await supabaseFetch('withdrawals', 'POST', {
            user_id: id,
            binance_id: binanceId,
            amount: withdrawAmount,
            status: 'requested',
            requested_at: dateNow
        });

        // 7. Success
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
// Main Handler Function (Entry Point)
// ------------------------------------------------------------------

/**
 * The main serverless function handler.
 */
module.exports = async (req, res) => {
  // Only allow POST requests
  if (req.method !== 'POST') {
    return sendError(res, 'Only POST requests are allowed.', 405);
  }

  // Parse the JSON body
  let body;
  try {
    body = await new Promise((resolve, reject) => {
      let data = '';
      req.on('data', chunk => { data += chunk; });
      req.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error('Invalid JSON format in request body.'));
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
    case 'completeTask': // ⬅️ The existing route is now dynamic
      await handleCompleteTask(req, res, body);
      break;
    case 'getTasks': // ⬅️ المسار الجديد
      await handleGetTasks(req, res, body);
      break;
    default:
      sendError(res, `Unknown request type: ${body.type}`, 400);
  }
}

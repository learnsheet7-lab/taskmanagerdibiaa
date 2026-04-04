require('dotenv').config();
const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const dayjs = require('dayjs');
const customParseFormat = require('dayjs/plugin/customParseFormat');
const timezone = require('dayjs/plugin/timezone'); // Add this
const utc = require('dayjs/plugin/utc');   
const axios = require('axios');        // Add this

dayjs.extend(customParseFormat);
dayjs.extend(utc);      // Add this
dayjs.extend(timezone); // Add this

// Set default timezone to IST
dayjs.tz.setDefault("Asia/Kolkata");
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
const multer = require('multer');
const csv = require('csv-parser');

const app = express();
app.use(express.json());
app.use(cors());
const upload = multer({ dest: '/tmp' });

// --- DATABASE CONNECTION ---
const db = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 20,
    queueLimit: 0,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0,
    dateStrings: true
});

// --- GOOGLE AUTH ---
const getAuth = () => {
    if (process.env.GOOGLE_CREDENTIALS) {
        try {
            const credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS);
            return new google.auth.GoogleAuth({ credentials, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
        } catch (e) {
            console.error("Error parsing GOOGLE_CREDENTIALS env var", e);
        }
    }
    if (fs.existsSync('credentials.json')) {
        return new google.auth.GoogleAuth({ keyFile: 'credentials.json', scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    }
    throw new Error("Google Credentials not found in ENV or File");
};

// --- HELPERS ---
const parseToMySQLDateTime = (d) => {
    if (!d) return null;
    const dt = dayjs(d.toString().trim(), ['DD/MM/YYYY HH:mm:ss', 'YYYY-MM-DD HH:mm:ss', 'MM/DD/YYYY HH:mm:ss', 'DD-MM-YYYY HH:mm:ss'], true);
    return dt.isValid() ? dt.format('YYYY-MM-DD HH:mm:ss') : null;
};
const parseToMySQLDate = (d) => {
    if (!d) return null;
    const dt = dayjs(d.toString().trim(), ['DD/MM/YYYY', 'YYYY-MM-DD', 'MM/DD/YYYY'], true);
    return dt.isValid() ? dt.format('YYYY-MM-DD') : null;
};
const parseDate = (d) => {
    if (!d || d.toString().trim() === "") return null;

    const dateStr = d.toString().trim();
    const formats = ['DD/MM/YYYY HH:mm:ss', 'YYYY-MM-DD HH:mm:ss', 'DD/MM/YYYY', 'YYYY-MM-DD'];

    // 1. First, create a basic dayjs object to check validity
    const dt = dayjs(dateStr, formats, true);

    // 2. If valid, convert it to the Asia/Kolkata timezone
    if (dt.isValid()) {
        return dayjs.tz(dateStr, formats, "Asia/Kolkata");
    }

    return null;
};
const addWorkdays = (startDate, days) => {
    if (!startDate) return null;

    // Handle both dayjs objects and raw strings
    let d = dayjs.isDayjs(startDate) ? startDate : dayjs.tz(startDate, "Asia/Kolkata");

    if (!d || !d.isValid()) return null;

    let added = 0;
    const daysToWait = Math.floor(days); // Ensure we handle whole days

    while (added < daysToWait) {
        d = d.add(1, 'day');
        if (d.day() !== 0) { // Skip Sundays
            added++;
        }
    }

    // If result falls on Sunday, move to Monday
    if (d.day() === 0) {
        d = d.add(1, 'day');
    }

    return d;
};

// whatsapp code 
//key_3UoCIEFWON
const sendWhatsApp = async (mobile, templateName, variables) => {
    if (!mobile || mobile.length < 10) return;
    
    // Clean the mobile number: remove any non-numeric characters
    const cleanMobile = mobile.replace(/\D/g, '');
    const formattedMobile = cleanMobile.startsWith('91') ? cleanMobile : `91${cleanMobile}`;

    const data = {
        "messages": [
            {
                "from": "919910690691", 
                "to": formattedMobile, // REMOVED the "+" sign here
                "content": {
                    "templateName": templateName,
                    "templateData": {
                        "body": {
                            "placeholders": variables
                        }
                    },
                    "language": "en"
                }
            }
        ]
    };

    try {
        const response = await axios({
            method: 'post',
            url: 'https://public.doubletick.io/whatsapp/message/template',
            headers: { 
                'Authorization': 'key_3UoCIEFWON',
                'Content-Type': 'application/json'
            },
            data: data
        });

        // This will print the DoubleTick Message ID or a failure reason
        console.log("DoubleTick Response:", JSON.stringify(response.data, null, 2));
        
    } catch (error) {
        // This will tell you if the template name is wrong or variables count is wrong
        console.error("DoubleTick Error Detail:", error.response?.data || error.message);
    }
};

// --- AUTH & USERS ---
app.post('/login', async (req, res) => { try { const [r] = await db.query("SELECT * FROM users WHERE (email = ? OR mobile = ?) AND password = ?", [req.body.identifier, req.body.identifier, req.body.password]); res.json(r.length ? r[0] : { message: "User not found" }); } catch (e) { res.status(500).json(e); } });
app.get('/users', async (req, res) => { const [r] = await db.query("SELECT * FROM users"); res.json(r); });
app.post('/users', async (req, res) => { await db.query("INSERT INTO users (name, role, department, email, mobile, password) VALUES (?,?,?,?,?,?)", [req.body.name, req.body.role, req.body.department, req.body.email, req.body.mobile, req.body.password]); res.json({ message: "Created" }); });
app.put('/users/update', async (req, res) => { const { name, email, role, department, mobile, id, password } = req.body; if (password && password.trim() !== "") await db.query("UPDATE users SET name=?, email=?, role=?, department=?, mobile=?, password=? WHERE id=?", [name, email, role, department, mobile, password, id]); else await db.query("UPDATE users SET name=?, email=?, role=?, department=?, mobile=? WHERE id=?", [name, email, role, department, mobile, id]); res.json({ message: "Updated" }); });
app.delete('/users/:id', async (req, res) => { await db.query("DELETE FROM users WHERE id=?", [req.params.id]); res.json({ message: "User Deleted" }); });
app.post('/users/change-password-secure', async (req, res) => { const { id, currentPassword, newPassword } = req.body; const [u] = await db.query("SELECT * FROM users WHERE id=? AND password=?", [id, currentPassword]); if (u.length === 0) return res.status(401).json({ message: "Current password incorrect" }); await db.query("UPDATE users SET password=? WHERE id=?", [newPassword, id]); res.json({ message: "Password Changed" }); });

// --- DASHBOARD ---
// --- HIGH-PERFORMANCE DASHBOARD ROUTE ---
app.get('/dashboard/:email/:role', async (req, res) => {
    try {
        const { email, role } = req.params;
        const { filterEmail } = req.query;

        const todayStr = dayjs().format('YYYY-MM-DD');
        const endOfToday = dayjs().endOf('day').format('YYYY-MM-DD HH:mm:ss');
        const targetEmail = (role === 'Admin' && filterEmail) ? filterEmail : email;
        const isFiltered = (role === 'Admin' && !filterEmail) ? false : true;

        // --- 1. OPTIMIZED DELEGATION & CHECKLIST SUMMARY (Combined into 1 Query) ---
        const base = !isFiltered ? "1=1" : `assigned_to_email='${targetEmail}'`;
        const chkBase = !isFiltered ? "1=1" : `employee_email='${targetEmail}'`;

        const summarySql = `
            SELECT 
                (SELECT COUNT(*) FROM tasks WHERE ${base} AND status IN ('Pending','Revision Requested','Waiting Approval')) as delPending,
                (SELECT COUNT(*) FROM tasks WHERE ${base} AND status IN ('Revised', 'Revision Requested')) as delRevised,
                (SELECT COUNT(*) FROM tasks WHERE ${base} AND status='Completed') as delCompleted,
                (SELECT COUNT(*) FROM checklist_tasks WHERE ${chkBase} AND target_date <= '${todayStr}') as chkTotal,
                (SELECT COUNT(*) FROM checklist_tasks WHERE ${chkBase} AND target_date <= '${todayStr}' AND status='Completed') as chkCompleted
        `;
        const [[summary]] = await db.query(summarySql);

        // --- 2. OPTIMIZED FMS SUMMARY ---
        let fmsStepFilter = "";
        if (isFiltered) {
            const [mySteps] = await db.query("SELECT step_id FROM fms_dibiaa_steps_config WHERE doer_emails LIKE ?", [`%${targetEmail}%`]);
            const ids = mySteps.length > 0 ? mySteps.map(s => s.step_id).join(',') : '0';
            fmsStepFilter = `AND t.step_id IN (${ids})`;
        }

        const fmsSummarySql = `
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status='Completed' THEN 1 ELSE 0 END) as completed
            FROM fms_dibiaa_tasks t
            WHERE 1=1 ${fmsStepFilter}
        `;
        const [[fmsSum]] = await db.query(fmsSummarySql);

        // --- 3. FETCH DATA LISTS IN PARALLEL ---
        // Using Promise.all here allows the database to process these three list fetches at once
        const [delTodayList, chkTodayList, fmsTodayList] = await Promise.all([
            db.query(`SELECT * FROM tasks WHERE ${base} AND target_date <= '${todayStr}' AND status!='Completed' ORDER BY target_date ASC`),
            db.query(`SELECT id, description, employee_name, target_date, status FROM checklist_tasks WHERE ${chkBase} AND target_date <= '${todayStr}' AND status='Pending' ORDER BY target_date ASC`),
            db.query(`
                SELECT t.id, t.plan_date, r.job_number, r.company_name, r.order_by, s.step_name 
                FROM fms_dibiaa_tasks t 
                JOIN fms_dibiaa_raw r ON t.job_id = r.job_id 
                JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id
                WHERE t.status='Pending' AND t.plan_date <= '${endOfToday}' 
                ${fmsStepFilter}
                ORDER BY t.plan_date ASC
            `)
        ]);

        // --- 4. COMBINED RESPONSE ---
        res.json({
            delegation: {
                pending: summary.delPending,
                revised: summary.delRevised,
                completed: summary.delCompleted,
                today: delTodayList[0]
            },
            checklist: {
                pending: chkTodayList[0].length,
                total: summary.chkTotal,
                completed: summary.chkCompleted,
                today: chkTodayList[0]
            },
            fms: {
                pending: fmsTodayList[0].length,
                total: fmsSum.total || 0,
                completed: fmsSum.completed || 0,
                today: fmsTodayList[0]
            }
        });
    } catch (err) {
        console.error("Optimized Dashboard Error:", err);
        res.status(500).json({ error: err.message });
    }
});

// --- CHECKLIST MODULE (FIXED EMAIL STORAGE) ---
app.post('/checklist', async (req, res) => {
    // Destructure using employee_email
    const { description, employee_email, employee_name, frequency, start_date } = req.body;

    console.log("Received Email:", employee_email); // Debugging line

    const [h] = await db.query("SELECT holiday_date FROM holidays");
    const holidays = new Set(h.map(x => dayjs(x.holiday_date).format('YYYY-MM-DD')));

    const tasks = [];
    let current = dayjs(start_date);
    const endOfYear = dayjs().endOf('year');

    while (current.isBefore(endOfYear) || current.isSame(endOfYear, 'day')) {
        const formattedDate = current.format('YYYY-MM-DD');
        const isSunday = current.day() === 0;

        if ((!isSunday || formattedDate === start_date) && !holidays.has(formattedDate)) {
            tasks.push([
                'CHK-' + Math.floor(Math.random() * 1000000),
                description,
                employee_email, // Index 2: matches column employee_email
                employee_name,
                frequency,
                formattedDate,
                'Pending'
            ]);
        }

        if (frequency === 'Daily') current = current.add(1, 'day');
        else if (frequency === 'Weekly') current = current.add(1, 'week');
        else if (frequency === 'Alternative Week') current = current.add(2, 'week');
        else if (frequency === 'Monthly') current = current.add(1, 'month');
        else if (frequency === 'Quarterly') current = current.add(3, 'month');
        else if (frequency === 'Yearly') current = current.add(1, 'year');
        else break;
    }

    if (tasks.length > 0) {
        // Ensure column order here matches the tasks.push order exactly
        const sql = "INSERT INTO checklist_tasks (uid, description, employee_email, employee_name, frequency, target_date, status) VALUES ?";
        await db.query(sql, [tasks]);
        res.json({ message: `Generated ${tasks.length} tasks` });
    } else {
        res.json({ message: "No tasks generated" });
    }
});

// FETCH CHECKLIST
app.get('/checklist/:email/:role', async (req, res) => {
    const today = dayjs().format('YYYY-MM-DD');
    let q = "";
    let params = [];

    if (req.params.role === 'Admin') {
        q = "SELECT * FROM checklist_tasks WHERE status != 'Completed' AND target_date <= ? ORDER BY target_date ASC";
        params = [today];
    } else {
        q = "SELECT * FROM checklist_tasks WHERE employee_email=? AND status != 'Completed' AND target_date <= ? ORDER BY target_date ASC";
        params = [req.params.email, today];
    }

    const [r] = await db.query(q, params);
    res.json(r);
});

app.put('/checklist/complete', async (req, res) => { await db.query("UPDATE checklist_tasks SET status='Completed', completed_at=NOW() WHERE id=?", [req.body.id]); res.json({ message: "Done" }); });

// --- TASKS MODULE ---
app.post('/tasks', async (req, res) => { 
    const { employee_name, email, approver_email, description, target_date, priority, approval_needed, assigned_by, remarks } = req.body;
    
    // 1. Insert into Database
    await db.query("INSERT INTO tasks (task_uid,employee_name,assigned_to_email,approver_email,description,target_date,priority,approval_needed,assigned_by,remarks,status,previous_status) VALUES (?,?,?,?,?,?,?,?,?,?,'Pending','Pending')", 
        ['T-' + Math.floor(Math.random() * 9000), employee_name, email, approver_email, description, target_date, priority, approval_needed, assigned_by || 'System', remarks || '']
    );

    // 2. WhatsApp Notification: delegation_1
    try {
        const [userRow] = await db.query("SELECT mobile FROM users WHERE email = ?", [email]);
        // Add this inside your post route temporarily to test
const testVars = ["Farhaan", "Fix the Sync Issue", "22-03-2026", "High"];
sendWhatsApp("7048462595", "delegation_1", testVars);
        if (userRow.length > 0 && userRow[0].mobile) {
            // {{1}} Name, {{2}} Desc, {{3}} Date, {{4}} Priority
            const vars = [employee_name, description, dayjs(target_date).format('DD/MM/YYYY'), priority];
            await sendWhatsApp(userRow[0].mobile, 'delegation_1', vars);
        }
    } catch (e) { console.error("WhatsApp Error:", e); }

    res.json({ message: "Delegated" }); 
});
app.get('/tasks/:email/:role', async (req, res) => { const q = req.params.role === 'Admin' ? "SELECT * FROM tasks ORDER BY created_at DESC" : "SELECT * FROM tasks WHERE assigned_to_email=? ORDER BY created_at DESC"; const [r] = await db.query(q, [req.params.email]); res.json(r); });
app.delete('/tasks/:id', async (req, res) => { await db.query("DELETE FROM tasks WHERE id=?", [req.params.id]); res.json({ message: "Deleted" }); });
app.put('/tasks/update-status', async (req, res) => {
    const { id, status, revised_date, remarks, is_rejection } = req.body;

    try {
        // 1. Fetch task details first for WhatsApp variables and current state
        const [taskData] = await db.query("SELECT * FROM tasks WHERE id = ?", [id]);
        if (taskData.length === 0) return res.status(404).json({ message: "Task not found" });
        const task = taskData[0];

        // 2. Handle Rejection (Revision Requested -> Back to previous)
        if (is_rejection) {
            await db.query(`UPDATE tasks SET status = CASE WHEN previous_status IS NULL OR previous_status='' OR previous_status='Waiting Approval' THEN 'Pending' ELSE previous_status END, completed_at = NULL WHERE id=?`, [id]);
            
            if (task.status === 'Revision Requested') {
                const [doer] = await db.query("SELECT mobile FROM users WHERE email = ?", [task.assigned_to_email]);
                if (doer[0]?.mobile) {
                    const vars = [task.employee_name, task.description, dayjs(task.target_date).format('DD/MM/YYYY'), dayjs(task.revised_date_request).format('DD/MM/YYYY'), 'Rejected'];
                    sendWhatsApp(doer[0].mobile, 'revised_request_status', vars);
                }
            }
        }
        // 3. Handle New Revision Request
        else if (status === 'Revision Requested') {
            await db.query("UPDATE tasks SET previous_status=status, status=?, revised_date_request=?, revision_remarks=? WHERE id=?", [status, revised_date, remarks, id]);
            
            const [admins] = await db.query("SELECT mobile, name FROM users WHERE role = 'Admin'");
            admins.forEach(admin => {
                if (admin.mobile) {
                    const vars = [admin.name, task.employee_name, task.description, dayjs(task.target_date).format('DD/MM/YYYY'), dayjs(revised_date).format('DD/MM/YYYY')];
                    sendWhatsApp(admin.mobile, 'revised_request', vars);
                }
            });
        }
        // 4. Handle Admin Approving Revision
        else if (status === 'Revised') {
            await db.query("UPDATE tasks SET status='Revised', target_date=revised_date_request WHERE id=?", [id]);
            
            const [doer] = await db.query("SELECT mobile FROM users WHERE email = ?", [task.assigned_to_email]);
            if (doer[0]?.mobile) {
                const vars = [task.employee_name, task.description, dayjs(task.target_date).format('DD/MM/YYYY'), dayjs(task.revised_date_request).format('DD/MM/YYYY'), 'Approved'];
                sendWhatsApp(doer[0].mobile, 'revised_request_status', vars);
            }
        }
        // --- NEW: 5. Handle ADMIN HOLD ---
        else if (status === 'Hold') {
            // Save the CURRENT status into previous_status before moving to Hold
            await db.query("UPDATE tasks SET previous_status = status, status = 'Hold' WHERE id = ?", [id]);
        }
        // --- NEW: 6. Handle ADMIN UNHOLD ---
        else if (status === 'Unhold') {
            // Restore status from previous_status and reset previous to Pending
            await db.query("UPDATE tasks SET status = previous_status, previous_status = 'Pending' WHERE id = ?", [id]);
        }
        // 7. General Status Updates (Completed, Waiting Approval, etc.)
        else {
            await db.query(`UPDATE tasks SET previous_status = status, status = ?, completed_at = CASE WHEN ? = 'Completed' THEN NOW() ELSE NULL END WHERE id = ?`, [status, status, id]);
            
            if (status === 'Completed' || status === 'Waiting Approval') {
                const [assignee] = await db.query("SELECT mobile, name FROM users WHERE name = ?", [task.assigned_by]);
                if (assignee[0]?.mobile) {
                    const vars = [assignee[0].name, task.employee_name, task.description, dayjs(task.target_date).format('DD/MM/YYYY')];
                    sendWhatsApp(assignee[0].mobile, 'task_approval', vars);
                }
            }
        }

        res.json({ message: "Updated" });
    } catch (error) {
        console.error("Update Status Error:", error);
        res.status(500).json({ message: "Internal Server Error" });
    }
});
app.put('/tasks/edit/:id', async (req, res) => { const { description, target_date, priority, approval_needed, remarks, assigned_to_email, approver_email, employee_name } = req.body; await db.query("UPDATE tasks SET description=?, target_date=?, priority=?, approval_needed=?, remarks=?, assigned_to_email=?, approver_email=?, employee_name=? WHERE id=?", [description, target_date, priority, approval_needed, remarks, assigned_to_email, approver_email, employee_name, req.params.id]); res.json({ message: "Task Updated" }); });
app.get('/comments/:taskId', async (req, res) => { const [r] = await db.query("SELECT id,task_id,user_name,comment,DATE_FORMAT(created_at,'%d/%m/%Y %H:%i:%s') as formatted_date FROM task_comments WHERE task_id=? ORDER BY created_at DESC", [req.params.taskId]); res.json(r); });
app.post('/comments', async (req, res) => { await db.query("INSERT INTO task_comments (task_id,user_name,comment) VALUES (?,?,?)", [req.body.task_id, req.body.user_name, req.body.comment]); res.json({ message: "Added" }); });
app.get('/approvals/:email', async (req, res) => { const [u] = await db.query("SELECT role FROM users WHERE email=?", [req.params.email]); let q = "SELECT * FROM tasks WHERE status IN ('Waiting Approval','Revision Requested')"; let params = []; if (u.length === 0 || u[0].role !== 'Admin') { q += " AND LOWER(approver_email)=LOWER(?)"; params.push(req.params.email); } const [r] = await db.query(q, params); res.json(r); });
app.get('/holidays', async (req, res) => { const [r] = await db.query("SELECT * FROM holidays ORDER BY holiday_date"); res.json(r); });
app.post('/holidays', async (req, res) => { await db.query("INSERT INTO holidays (holiday_date, name) VALUES (?,?)", [req.body.date, req.body.name]); res.json({ message: "Added" }); });
app.get('/mis/tasks', async (req, res) => { const [rows] = await db.query("SELECT description,target_date,status,completed_at FROM tasks WHERE assigned_to_email=? AND target_date BETWEEN ? AND ? ORDER BY target_date", [req.query.email, req.query.start, req.query.end]); res.json(rows); });
app.post('/mis/plan', async (req, res) => { const { email, date, count } = req.body; const [ex] = await db.query("SELECT id FROM employee_plans WHERE employee_email=? AND plan_date=?", [email, date]); if (ex.length > 0) await db.query("UPDATE employee_plans SET planned_count=? WHERE id=?", [count, ex[0].id]); else await db.query("INSERT INTO employee_plans (employee_email,plan_date,planned_count) VALUES (?,?,?)", [email, date, count]); res.json({ message: "Saved" }); });

// 1. FMS Task Tracker by Job Number
app.get('/fms/track/:job_number', async (req, res) => {
    const { job_number } = req.params;
    const sql = `
        SELECT t.status, t.actual_date as actual_time, s.step_name 
        FROM fms_dibiaa_tasks t
        JOIN fms_dibiaa_raw r ON t.job_id = r.job_id
        JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id
        WHERE r.job_number = ?
        ORDER BY t.step_id ASC`;
    const [rows] = await db.query(sql, [job_number]);
    res.json(rows);
});

// 2. FMS Status Summary Report
app.get('/fms/report-summary', async (req, res) => {
    const { start, end } = req.query;
    const sql = `
        SELECT 
            s.step_name,
            COUNT(t.id) as \`Total\`,
            SUM(CASE 
                WHEN t.actual_date IS NULL OR t.actual_date = '' OR t.actual_date = '0000-00-00 00:00:00' 
                THEN 1 ELSE 0 
            END) as \`Pending\`,
            SUM(CASE 
                WHEN t.actual_date IS NOT NULL 
                  AND t.actual_date != '' 
                  AND t.actual_date != '0000-00-00 00:00:00'
                  AND DATE(t.actual_date) <= DATE(t.plan_date) 
                THEN 1 ELSE 0 
            END) as \`Ontime\`,
            SUM(CASE 
                WHEN t.actual_date IS NOT NULL 
                  AND t.actual_date != '' 
                  AND t.actual_date != '0000-00-00 00:00:00'
                  AND DATE(t.actual_date) > DATE(t.plan_date) 
                THEN 1 ELSE 0 
            END) as \`Delayed\`
        FROM fms_dibiaa_tasks t
        JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id
        WHERE t.plan_date >= ? AND t.plan_date <= ?
        GROUP BY s.step_id, s.step_name
        ORDER BY s.step_id ASC`;

    try {
        // ✅ FIX: force full day range — start at 00:00:00, end at 23:59:59
        const from = `${start} 00:00:00`;
        const to   = `${end} 23:59:59`;

        const [rows] = await db.query(sql, [from, to]);
        res.json(rows);
    } catch (error) {
        console.error("SQL Error in FMS Report:", error);
        res.status(500).json({ error: "Internal Server Error", details: error.message });
    }
});

// --- UPLOADS ---
app.post('/tasks/upload', upload.single('file'), async (req, res) => { if (!req.file) return res.status(400).json({ message: "No file uploaded" }); try { const results = []; fs.createReadStream(req.file.path).pipe(csv()).on('data', (data) => results.push(data)).on('end', async () => { const [users] = await db.query("SELECT email, name FROM users"); const bulkTasks = []; const assignedBy = req.body.assigned_by || 'Admin Upload'; for (const row of results) { const empName = users.find(u => u.email === row.employee_email)?.name || row.employee_email; const tDate = parseToMySQLDate(row.target_date); if (tDate) { bulkTasks.push(['T-' + Math.floor(Math.random() * 90000), empName, row.employee_email, row.approver_email, row.description, tDate, row.priority || 'Medium', row.approval_needed || 'No', assignedBy, row.remarks || '', 'Pending', 'Pending']); } } if (bulkTasks.length > 0) { await db.query("INSERT INTO tasks (task_uid,employee_name,assigned_to_email,approver_email,description,target_date,priority,approval_needed,assigned_by,remarks,status,previous_status) VALUES ?", [bulkTasks]); } fs.unlinkSync(req.file.path); res.json({ message: `Delegated ${bulkTasks.length} tasks.` }); }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/checklist/upload', upload.single('file'), async (req, res) => {
    if (!req.file) return res.status(400).json({ message: "No file uploaded" });
    try {
        const results = [];
        fs.createReadStream(req.file.path).pipe(csv()).on('data', (data) => results.push(data)).on('end', async () => {
            const [h] = await db.query("SELECT holiday_date FROM holidays");
            const holidays = new Set(h.map(x => dayjs(x.holiday_date).format('YYYY-MM-DD')));
            const [users] = await db.query("SELECT email, name FROM users");
            const bulkTasks = [];
            const endOfYear = dayjs().endOf('year');
            for (const row of results) {
                const targetEmail = row.employee_email;
                const userMatch = users.find(u => u.email === targetEmail);
                const empName = userMatch?.name || targetEmail;
                const startDateStr = parseToMySQLDate(row.start_date);
                if (startDateStr) {
                    let current = dayjs(startDateStr);
                    while (current.isBefore(endOfYear) || current.isSame(endOfYear, 'day')) {
                        const formattedDate = current.format('YYYY-MM-DD');
                        const isSunday = current.day() === 0;
                        const isHoliday = holidays.has(formattedDate);
                        if ((!isSunday || formattedDate === startDateStr) && !isHoliday) {
                            bulkTasks.push(['CHK-' + Math.floor(Math.random() * 1000000), row.description, targetEmail, empName, row.frequency, formattedDate, 'Pending']);
                        }
                        if (row.frequency === 'Daily') current = current.add(1, 'day');
                        else if (row.frequency === 'Weekly') current = current.add(1, 'week');
                        else if (row.frequency === 'Monthly') current = current.add(1, 'month');
                        else if (row.frequency === 'Quarterly') current = current.add(3, 'month');
                        else { current = current.add(1, 'year'); if (row.frequency !== 'Yearly') break; }
                    }
                }
            }
            if (bulkTasks.length > 0) {
                const chunkSize = 1000;
                for (let i = 0; i < bulkTasks.length; i += chunkSize) {
                    await db.query("INSERT INTO checklist_tasks (uid,description,employee_email,employee_name,frequency,target_date,status) VALUES ?", [bulkTasks.slice(i, i + chunkSize)]);
                }
            }
            fs.unlinkSync(req.file.path);
            res.json({ message: `Bulk Processed ${bulkTasks.length} tasks.` });
        });
    } catch (e) {
        if (req.file) fs.unlinkSync(req.file.path);
        res.status(500).json({ error: e.message });
    }
});

// DELETE CHECKLIST TASK
app.delete('/checklist/:id', async (req, res) => {
    const { id } = req.params;
    const { deleteAll } = req.query;

    try {
        if (deleteAll === 'true') {
            const [rows] = await db.query("SELECT description, employee_email FROM checklist_tasks WHERE id = ?", [id]);
            if (rows && rows.length > 0) {
                const { description, employee_email } = rows[0];
                await db.query("DELETE FROM checklist_tasks WHERE description = ? AND employee_email = ?", [description, employee_email]);
                return res.json({ message: "All recurring tasks deleted" });
            } else {
                return res.status(404).json({ message: "Task group not found" });
            }
        } else {
            const [result] = await db.query("DELETE FROM checklist_tasks WHERE id = ?", [id]);
            if (result.affectedRows === 0) {
                return res.status(404).json({ message: "Individual task not found" });
            }
            return res.json({ message: "Task deleted" });
        }
    } catch (e) {
        console.error("Delete Error:", e);
        return res.status(500).json({ error: e.message });
    }
});

// --- MIS REPORT ---
app.get('/mis/report', async (req, res) => {
    const { start, end } = req.query;
    const sql = `
        SELECT u.name as employee_name, u.email as employee_email,
        COALESCE((
            SELECT planned_count 
            FROM employee_plans 
            WHERE employee_email=u.email 
            AND plan_date <= ? 
            ORDER BY plan_date DESC 
            LIMIT 1
        ), 0) as planned,
        COUNT(t.id) as total_task,
        SUM(CASE WHEN t.status IN ('Pending','Revision Requested','Waiting Approval') THEN 1 ELSE 0 END) as total_pending,
        SUM(CASE WHEN t.status='Revised' THEN 1 ELSE 0 END) as total_revised,
        SUM(CASE WHEN t.status='Completed' THEN 1 ELSE 0 END) as total_completed 
        FROM users u 
        LEFT JOIN tasks t ON u.email=t.assigned_to_email AND t.target_date BETWEEN ? AND ? 
        WHERE u.role!='Admin' 
        GROUP BY u.email,u.name 
        HAVING total_task > 0 OR planned > 0`;
    const [rows] = await db.query(sql, [end, start, end]);
    res.json(rows);
});

app.post('/fms/sync-dibiaa', async (req, res) => {
    try {
        const SHEET_ID = '1C3qHR_jbjHgOQCM7MwRB4AZXtuY2W9jLpqIAEbYFWkQ';
        const auth = getAuth();
        if (!auth) return res.status(500).json({ error: "Google Auth Failed" });

        const sheets = google.sheets({ version: 'v4', auth });
        const response = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `fmstask!A2:O` });
        const rows = response.data.values;
        if (!rows || rows.length === 0) return res.json({ message: "No data found" });

        // 1. UPDATE RAW DATA
        const rawValues = rows.map((r, i) => [
            2 + i, parseToMySQLDateTime(r[0]), r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], parseToMySQLDate(r[13]), r[14]
        ]);

        const rawSql = `INSERT INTO fms_dibiaa_raw (sheet_row_index, timestamp, otd_type, job_number, order_by, company_name, box_type, box_style, box_color, printing_type, printing_color, specification, city, quantity, lead_time, repeat_new) 
                        VALUES ? 
                        ON DUPLICATE KEY UPDATE 
                        otd_type=VALUES(otd_type), box_type=VALUES(box_type), printing_type=VALUES(printing_type), 
                        quantity=VALUES(quantity), company_name=VALUES(company_name), box_style=VALUES(box_style),
                        box_color=VALUES(box_color), printing_color=VALUES(printing_color), specification=VALUES(specification),
                        city=VALUES(city), lead_time=VALUES(lead_time), repeat_new=VALUES(repeat_new)`;
        await db.query(rawSql, [rawValues]);

        // 2. PREPARE MAPS
        const [jobs] = await db.query("SELECT job_id, sheet_row_index FROM fms_dibiaa_raw");
        const jobMap = {};
        jobs.forEach(j => jobMap[j.sheet_row_index] = j.job_id);

        // Add 'status' to the query
        const [allTasks] = await db.query("SELECT id, job_id, step_id, actual_date, status FROM fms_dibiaa_tasks");
        const taskMap = {};
        allTasks.forEach(t => {
            taskMap[`${t.job_id}_${t.step_id}`] = {
                id: t.id,
                status: t.status, // Store the current status (Hold/Pending/Completed)
                actual: t.actual_date ? dayjs.tz(t.actual_date, "Asia/Kolkata") : null
            };
        });

        const taskUpdates = [];
        const tasksToDelete = [];

        // 3. RE-EVALUATE LOGIC
        for (let i = 0; i < rows.length; i++) {
            const r = rows[i];
            const rowIndex = 2 + i;
            const jobId = jobMap[rowIndex];
            if (!jobId) continue;

            const getAct = (s) => taskMap[`${jobId}_${s}`]?.actual || null;

            const A = parseDate(r[0]);
            const B = r[1];
            const F = r[5];
            const G = r[6];
            const I = r[8];
            const K = r[10];
            const N = parseDate(r[13]);

            const hasInner = K && K.toLowerCase().includes('inner print') || K.toLowerCase().includes('inner screen print');
            const hasReadystock = K && K.toLowerCase().includes('ready stock') || K.toLowerCase().includes('ready to stock');

            const isOffsetFoil = (I === 'Offset Print' || I === 'Foil Print' || I === 'No');
            const isScreenPrint = (I === 'Screen print');

            let plans = {};

            // --- START LOGIC ---
            if (A) plans[4] = addWorkdays(A, 3);  //step4

            const step4Act = getAct(4);
            if ((B === 'OTD' || B === 'Jewellery (OTD)' && step4Act)) plans[1] = addWorkdays(step4Act, 6); // step1

            if (B === 'OTD' || B === 'Jewellery (OTD)') {
                plans[2] = addWorkdays(getAct(1), 1);
            } else if (getAct(4)) {
                plans[2] = addWorkdays(getAct(4), 1);
            }
            //step2
            if (!hasReadystock) {
                if (!(F === 'Paper Box' || F === 'Foam')) {
                    if (getAct(2)) plans[3] = addWorkdays(getAct(2), 1);  //step3
                }
            }


            // const step1Act = getAct(1);
            // if ((B === 'OTD' || B === 'Jewellery (OTD)') && step1Act) {
            //     plans[2] = addWorkdays(step1Act, 1);
            // } else if ((B !== 'OTD' || B !== 'Jewellery (OTD)') && I === 'No' && A) {
            //     plans[2] = addWorkdays(A, 1);
            // } else if ((B !== 'OTD' || B !== 'Jewellery (OTD)') && I !== 'No' && step4Act){
            //     plans[2] = addWorkdays(step4Act, 1);
            // }


            if (!hasReadystock) {
                if (!(F === 'Paper Bag' || F === 'Paper Box' || F === 'PVC Pad' || (F || '').endsWith('Tray'))) {
                    if (getAct(2)) plans[5] = addWorkdays(getAct(2), 4);
                }
            }


            // step6-foiling
            if (!hasReadystock) {
                if (F === 'Paper Bag' && I === 'Foil Print' && getAct(7)) {
                    plans[6] = addWorkdays(getAct(7), 3);
                }
                else if (F !== 'Paper Bag' && I === 'Foil Print' && getAct(3)) { plans[6] = addWorkdays(getAct(3), 3); }
            }

            // step7 - die cutting
            if (!hasReadystock) {
                if (F === 'PVC Pad' && getAct(11)) {
                    plans[7] = addWorkdays(getAct(11), 3);
                }
                else if (F === 'Paper Box' && getAct(2)) {
                    plans[7] = addWorkdays(getAct(2), 3);
                } else if (I === 'Foil Print' && F === 'Paper Bag' && getAct(3)) {
                    plans[7] = addWorkdays(getAct(3), 3);
                }
                else if (F !== 'PVC Pad' && I !== 'Foil Print' && getAct(3)) {
                    plans[7] = addWorkdays(getAct(3), 3);
                }
                else if (F !== 'PVC Pad' && getAct(6)) {
                    plans[7] = addWorkdays(getAct(6), 3);

                }
            }

            // step 8 - full kitting
            if (!hasReadystock) {
                if (F !== 'PVC Pad') {
                    if (F === 'Paper Bag' && I === 'Foil Print' && getAct(6)) {
                        plans[8] = addWorkdays(getAct(6), 3);
                    }
                    else if (F === 'Paper Bag' && I !== 'Foil Print' && getAct(7)) {
                        plans[8] = addWorkdays(getAct(7), 1);
                    }
                    else if (F !== 'Paper Bag' && I !== 'Foil Print' && getAct(7)) {
                        plans[8] = addWorkdays(getAct(7), 1);
                    } else if (getAct(7)) {
                        plans[8] = addWorkdays(getAct(7), 1);
                    }
                }
            }

            // if (getAct(8)) {
            //     const condition = (G === 'Magnetic' || (G || '').startsWith('Sliding Handle') && I === 'Screen print') || (G === 'Magnetic' && isOffsetFoil && hasInner) || (G === 'Magnetic' && hasInner && I === 'Screen print');
            //     if (condition) plans[9] = addWorkdays(getAct(8), 1);
            // }

            // Case 1: (Magnetic OR Sliding) AND Screen Print
            const case1 = (G === 'Magnetic' || (G || '').startsWith('Sliding Handle')) && I === 'Screen print';

            // Case 2: Magnetic AND Inner AND Offset Foil
            const case2 = G === 'Magnetic' && hasInner && isOffsetFoil;

            // Case 3: Magnetic AND Inner AND Screen Print
            const case3 = G === 'Magnetic' && hasInner && I === 'Screen print';

            // Apply the update if any of the cases are true
            if (getAct(8) && (case1 || case2 || case3)) {
                plans[9] = addWorkdays(getAct(8), 1);
            }

            const isTopBottom = G === 'Top-Bottom';
            const isSlidingBox = G === 'Sliding Box';
            const isMagnetic = G === 'Magnetic';
            const isSlidingHandle = G === 'Sliding Handle Box';
            const isPaperBag = F === 'Paper Bag';

            let targetDate10 = null;
            if (F === 'Paper Box' && getAct(8)) targetDate10 = getAct(8);
            else if (isPaperBag && isScreenPrint) targetDate10 = getAct(12);
            else if (isPaperBag && isOffsetFoil) targetDate10 = getAct(8);
            else if (isMagnetic && hasInner) targetDate10 = getAct(11);
            else if ((isMagnetic || isSlidingHandle) && isOffsetFoil) targetDate10 = getAct(8);
            else if ((isMagnetic || isSlidingHandle) && isScreenPrint) targetDate10 = getAct(12);
            else if (isTopBottom && hasInner) targetDate10 = getAct(11);
            else if (isTopBottom || isSlidingBox) targetDate10 = getAct(8);
            if (targetDate10) plans[10] = addWorkdays(targetDate10, 2);

            // const base11 = getAct(10) || getAct(9) || getAct(8);
            // if (base11 && hasInner) plans[11] = addWorkdays(base11, 1);

            const innercase1 = isTopBottom && hasInner;
            const innercase2 = isMagnetic && hasInner && I === 'Screen print';
            const innercase3 = isMagnetic && hasInner && isOffsetFoil;
            const innercase4 = F === 'PVC Pad';


            if (innercase4) {
                plans[11] = addWorkdays(getAct(3), 1);
            }
            else if (innercase1) {
                plans[11] = addWorkdays(getAct(8), 1);
            } else if (innercase2) {
                plans[11] = addWorkdays(getAct(12), 1);
            } else if (innercase3) {
                plans[11] = addWorkdays(getAct(9), 1);
            }


            // Step12 - Screen Printing

            let targetDate12 = null;

            if (hasReadystock && I === 'Screen print') targetDate12 = getAct(2);
            else if (F === 'PVC Pad') targetDate12 = getAct(7);
            else if (isPaperBag && isScreenPrint) targetDate12 = getAct(8);
            else if ((isMagnetic || isSlidingHandle) && I === 'Screen print') targetDate12 = getAct(9);
            else if ((isMagnetic && hasInner) && I === 'Screen print') targetDate12 = getAct(9);
            else if ((isTopBottom && hasInner) && I === 'Screen print') targetDate12 = getAct(10);
            else if ((isTopBottom || isSlidingBox) && I === 'Screen print') targetDate12 = getAct(10);
            if (targetDate12) plans[12] = addWorkdays(targetDate12, 1);




            const isboxtypecon = (F === 'Cards' || F === 'Hooks');
            // const base13 = getAct(12) || getAct(11) || getAct(10);
            if (hasReadystock && I === 'No') plans[13] = addWorkdays(getAct(2), 1);
            else if (isboxtypecon) plans[13] = addWorkdays(getAct(2), 1);
            else if (F === 'Foam' && getAct(5)) plans[13] = addWorkdays(getAct(5), 1);
            else if (F === 'Paper Box' && getAct(10)) plans[13] = addWorkdays(getAct(10), 1);
            else if (isPaperBag && isScreenPrint) plans[13] = addWorkdays(getAct(10), 1);
            else if (isPaperBag && isOffsetFoil) plans[13] = addWorkdays(getAct(10), 1);
            else if (isMagnetic && hasInner && isOffsetFoil) plans[13] = addWorkdays(getAct(10), 1);
            else if ((isMagnetic || isSlidingHandle) && isOffsetFoil) plans[13] = addWorkdays(getAct(10), 1);
            else if ((isMagnetic || isSlidingHandle) && isScreenPrint) plans[13] = addWorkdays(getAct(10), 1);
            else if (isTopBottom && hasInner && isOffsetFoil) plans[13] = addWorkdays(getAct(10), 1);
            else if (F === 'PVC Pad' && getAct(12)) plans[13] = addWorkdays(getAct(12), 1);
            else if (isTopBottom && hasInner && isScreenPrint) plans[13] = addWorkdays(getAct(12), 1);
            else if ((isTopBottom || isSlidingBox) && isOffsetFoil) plans[13] = addWorkdays(getAct(10), 1);
            else if ((isTopBottom || isSlidingBox) && isScreenPrint) plans[13] = addWorkdays(getAct(12), 1);






            if (getAct(13)) plans[14] = addWorkdays(getAct(13), 1);
            if (getAct(14)) plans[15] = addWorkdays(getAct(14), 1);

            if (N) plans[16] = N;

            // --- 4. DATA SYNCHRONIZATION WITH "ACTUAL" CHECK ---
            for (let s = 1; s <= 16; s++) {
                const key = `${jobId}_${s}`;
                const existingTask = taskMap[key]; // Check what's currently in DB

                if (plans[s]) {
                    const sqlDate = plans[s].format('YYYY-MM-DD HH:mm:ss');

                    // LOGIC: If it already exists in DB and is on 'Hold', keep 'Hold'. 
                    // Otherwise, use 'Pending'.
                    const currentStatus = (existingTask && existingTask.status === 'Hold') ? 'Hold' : 'Pending';

                    taskUpdates.push([jobId, s, sqlDate, currentStatus]);
                } else {
                    // Safety: Only delete if it exists and hasn't been completed
                    if (existingTask && !existingTask.actual) {
                        tasksToDelete.push(existingTask.id);
                    }
                }
            }
        }

        // 5. EXECUTE DATABASE CHANGES
        // 5. EXECUTE DATABASE CHANGES
        if (tasksToDelete.length > 0) {
            await db.query("DELETE FROM fms_dibiaa_tasks WHERE id IN (?)", [tasksToDelete]);
        }

        if (taskUpdates.length > 0) {
            const taskSql = `
    INSERT INTO fms_dibiaa_tasks (job_id, step_id, plan_date, status) 
    VALUES ? 
    ON DUPLICATE KEY UPDATE 
    plan_date = IF(status = 'Completed', plan_date, VALUES(plan_date)),
    status = IF(status = 'Hold' OR status = 'Completed', status, VALUES(status)),
    hold_reason = hold_reason,
    hold_timestamp = hold_timestamp,
    unhold_timestamp = unhold_timestamp`;

            await db.query(taskSql, [taskUpdates]);
        }

        res.json({ message: `Sync Complete. Deleted ${tasksToDelete.length} obsolete tasks.` });

    } catch (e) {
        console.error("Sync Error:", e);
        res.status(500).json({ error: e.message });
    }
});

app.post('/fms/reset-production-job', async (req, res) => {
    const { job_number } = req.body;

    try {
        // 1. Get the internal job_id for the job number
        const [job] = await db.query("SELECT job_id FROM fms_dibiaa_raw WHERE job_number = ?", [job_number]);
        if (job.length === 0) return res.status(404).json({ message: "Job Number not found" });
        const jobId = job[0].job_id;

        // 2. THE RESET: Clear EVERY actual_date for this job.
        // This effectively "unticks" the Done box for every step.
        // We keep Step 16 (Dispatch) safe as per your previous preference.
        await db.query(`
            UPDATE fms_dibiaa_tasks 
            SET actual_date = NULL, status = 'Pending' 
            WHERE job_id = ? AND step_id != 16`,
            [jobId]
        );

        res.json({ message: "Job history cleared successfully." });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: e.message });
    }
});

app.get('/fms/dibiaa-tasks', async (req, res) => {
    try {
        const { email, role } = req.query;
        const [configs] = await db.query("SELECT * FROM fms_dibiaa_steps_config");

        const relevantSteps = role === 'Admin'
            ? configs
            : configs.filter(c => c.doer_emails && c.doer_emails.includes(email));

        const stepIds = relevantSteps.map(s => s.step_id);
        if (stepIds.length === 0) return res.json({});

        // CRITICAL CHANGE: We JOIN tasks (t) with raw (r) using job_id
        const [tasks] = await db.query(`
    SELECT 
        t.*, 
        r.job_number, r.company_name, r.box_type, r.quantity as total_qty, 
        r.timestamp, r.otd_type, r.order_by, r.box_style, r.box_color, 
        r.printing_type, r.printing_color, r.specification, r.city, 
        r.lead_time, r.repeat_new,
        s.step_name, s.visible_columns,
        -- Fetch the contractor name saved in Step 8 for this Job ID
        (SELECT custom_field_1 FROM fms_dibiaa_tasks WHERE job_id = t.job_id AND step_id = 8 LIMIT 1) as step8_contractor,
        (SELECT custom_field_1 FROM fms_dibiaa_tasks WHERE job_id = t.job_id AND (custom_field_1 IS NOT NULL AND custom_field_1 != '') ORDER BY actual_date DESC LIMIT 1) as latest_worker,
        (SELECT custom_field_2 FROM fms_dibiaa_tasks WHERE job_id = t.job_id AND (custom_field_2 IS NOT NULL AND custom_field_2 != '') ORDER BY actual_date DESC LIMIT 1) as latest_qty
    FROM fms_dibiaa_tasks t 
    JOIN fms_dibiaa_raw r ON t.job_id = r.job_id 
    JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id 
    WHERE (t.status = 'Pending' OR t.status = 'Hold') AND t.step_id IN (?)
    ORDER BY t.plan_date ASC`, [stepIds]);

        const grouped = {};
        relevantSteps.forEach(s => {
            const stepTasks = tasks.filter(t => t.step_id === s.step_id);
            if (stepTasks.length > 0) grouped[s.step_name] = stepTasks;
        });

        res.json(grouped);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/fms/dibiaa-complete', async (req, res) => {
    const { task_id, delay_reason, contractor, printer, qty } = req.body;
    const now = dayjs().format('YYYY-MM-DD HH:mm:ss');
    // Explicitly get the current time in IST
    const nowIST = dayjs().tz("Asia/Kolkata").format('YYYY-MM-DD HH:mm:ss');

    try {
        // 1. Fetch plan date to calculate delay
        const [t] = await db.query("SELECT plan_date FROM fms_dibiaa_tasks WHERE id=?", [task_id]);
        if (!t || t.length === 0) return res.status(404).json({ message: "Task not found" });

        const plan = dayjs(t[0].plan_date);
        const delayHrs = dayjs().diff(plan, 'hour');

        // 2. Determine which name to save (Contractor or Printer)
        const workerName = contractor || printer || '';

        // 3. Update database with ALL fields
        await db.query(
            "UPDATE fms_dibiaa_tasks SET status='Completed', actual_date=?, delay_hours=?, delay_reason=?, custom_field_1=?, custom_field_2=? WHERE id=?",
            [nowIST, delayHrs > 0 ? delayHrs : 0, delay_reason, contractor || printer, qty, task_id]
        );
        res.json({ message: "Task Completed" });

    } catch (error) {
        console.error("Error completing FMS task:", error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

app.get('/fms/rolling-report', async (req, res) => {
    try {
        const sql = `
            SELECT 
                r.job_number, 
                r.order_by, 
                s.step_name, 
                s.step_id,
                t.plan_date, 
                t.actual_date,
                r.company_name, 
                r.quantity, 
                r.city
            FROM fms_dibiaa_tasks t
            JOIN fms_dibiaa_raw r ON t.job_id = r.job_id
            JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id
            WHERE t.step_id >= 1
              AND t.step_id <= 13
              AND t.plan_date IS NOT NULL
              AND t.plan_date > '1000-01-01'
              AND (t.actual_date IS NULL OR t.actual_date = '' OR t.actual_date = '0000-00-00 00:00:00')
            ORDER BY t.plan_date ASC`;

        const [rows] = await db.query(sql);

        // Calculate stats
        const uniqueJobs = [...new Set(rows.map(r => r.job_number))];
        const uniqueClients = [...new Set(rows.map(r => r.company_name))];
        const totalQty = rows.reduce((acc, curr, idx, self) => {
            const isFirst = self.findIndex(t => t.job_number === curr.job_number) === idx;
            return isFirst ? acc + (Number(curr.quantity) || 0) : acc;
        }, 0);

        res.json({
            data: rows,
            stats: {
                totalJobs: uniqueJobs.length,
                uniqueClients: uniqueClients.length,
                totalQty
            }
        });
    } catch (error) {
        console.error("Rolling Report Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// --- SHARED FMS SYNC FUNCTION ---
// This function recalculates plans for all jobs based on current actual dates
// --- SHARED FMS SYNC FUNCTION ---
const performFmsSync = async () => {
    const SHEET_ID = '1C3qHR_jbjHgOQCM7MwRB4AZXtuY2W9jLpqIAEbYFWkQ';
    const auth = getAuth();
    const sheets = google.sheets({ version: 'v4', auth });
    const response = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `fmstask!A2:O` });
    const rows = response.data.values;
    if (!rows || rows.length === 0) return { message: "No data found" };

    // 1. Refresh Job Map and Task Map
    const [jobs] = await db.query("SELECT job_id, sheet_row_index FROM fms_dibiaa_raw");
    const jobMap = {};
    jobs.forEach(j => jobMap[j.sheet_row_index] = j.job_id);

    // --- CRITICAL CHANGE 1: Fetch 'status' in the query ---
    const [allTasks] = await db.query("SELECT id, job_id, step_id, actual_date, status FROM fms_dibiaa_tasks");
    const taskMap = {};
    allTasks.forEach(t => {
        taskMap[`${t.job_id}_${t.step_id}`] = {
            id: t.id,
            status: t.status, // Now storing current status
            actual: t.actual_date ? dayjs.tz(t.actual_date, "Asia/Kolkata") : null
        };
    });

    const taskUpdates = [];
    const tasksToDelete = [];

    // 2. RE-EVALUATE EVERY ROW LOGICALLY
    for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const jobId = jobMap[2 + i];
        if (!jobId) continue;

        const getAct = (s) => taskMap[`${jobId}_${s}`]?.actual || null;

        const A = parseDate(r[0]); const B = r[1]; const F = r[5]; const G = r[6]; const I = r[8]; const K = r[10]; const N = parseDate(r[13]);
        const hasInner = K && K.toLowerCase().includes('inner print') || K.toLowerCase().includes('inner screen print');
        const hasReadystock = K && K.toLowerCase().includes('ready stock') || K.toLowerCase().includes('ready to stock');

        const isOffsetFoil = (I === 'Offset Print' || I === 'Foil Print' || I === 'No');
        const isScreenPrint = (I === 'Screen print');

        let plans = {};

        // --- PRODUCTION LOGIC START (Your existing rules) ---
        if (A) plans[4] = addWorkdays(A, 3);
        const step4Act = getAct(4);
        if ((B === 'OTD' || B === 'Jewellery (OTD)' && step4Act)) plans[1] = addWorkdays(step4Act, 6);
        if (B === 'OTD' || B === 'Jewellery (OTD)') {
            plans[2] = addWorkdays(getAct(1), 1);
        } else if (getAct(4)) {
            plans[2] = addWorkdays(getAct(4), 1);
        }
        if (!hasReadystock) {
            if (!(F === 'Paper Box' || F === 'Foam')) {
                if (getAct(2)) plans[3] = addWorkdays(getAct(2), 1);
            }
        }
        if (!hasReadystock) {
            if (!(F === 'Paper Bag' || F === 'Paper Box' || F === 'PVC Pad' || (F || '').endsWith('Tray'))) {
                if (getAct(2)) plans[5] = addWorkdays(getAct(2), 4);
            }
        }
        if (!hasReadystock) {
            if (F === 'Paper Bag' && I === 'Foil Print' && getAct(7)) {
                plans[6] = addWorkdays(getAct(7), 3);
            } else if (F !== 'Paper Bag' && I === 'Foil Print' && getAct(3)) {
                plans[6] = addWorkdays(getAct(3), 3);
            }
        }
        if (!hasReadystock) {
            if (F === 'PVC Pad' && getAct(11)) {
                plans[7] = addWorkdays(getAct(11), 3);
            } else if (F === 'Paper Box' && getAct(2)) {
                plans[7] = addWorkdays(getAct(2), 3);
            } else if (I === 'Foil Print' && F === 'Paper Bag' && getAct(3)) {
                plans[7] = addWorkdays(getAct(3), 3);
            } else if (F !== 'PVC Pad' && I !== 'Foil Print' && getAct(3)) {
                plans[7] = addWorkdays(getAct(3), 3);
            } else if (F !== 'PVC Pad' && getAct(6)) {
                plans[7] = addWorkdays(getAct(6), 3);
            }
        }
        if (!hasReadystock) {
            if (F !== 'PVC Pad') {
                if (F === 'Paper Bag' && I === 'Foil Print' && getAct(6)) {
                    plans[8] = addWorkdays(getAct(6), 3);
                } else if (F !== 'Paper Bag' && I !== 'Foil Print' && getAct(7)) {
                    plans[8] = addWorkdays(getAct(7), 1);
                } else if (getAct(7)) {
                    plans[8] = addWorkdays(getAct(7), 1);
                }
            }
        }
        const case1 = (G === 'Magnetic' || (G || '').startsWith('Sliding Handle')) && I === 'Screen print';
        const case2 = G === 'Magnetic' && hasInner && isOffsetFoil;
        const case3 = G === 'Magnetic' && hasInner && I === 'Screen print';
        if (getAct(8) && (case1 || case2 || case3)) {
            plans[9] = addWorkdays(getAct(8), 1);
        }
        const isTopBottom = G === 'Top-Bottom'; const isSlidingBox = G === 'Sliding Box'; const isMagnetic = G === 'Magnetic'; const isSlidingHandle = G === 'Sliding Handle Box'; const isPaperBag = F === 'Paper Bag';
        let targetDate10 = null;
        if (F === 'Paper Box' && getAct(8)) targetDate10 = getAct(8);
        else if (isPaperBag && isScreenPrint) targetDate10 = getAct(12);
        else if (isPaperBag && isOffsetFoil) targetDate10 = getAct(8);
        else if (isMagnetic && hasInner) targetDate10 = getAct(11);
        else if ((isMagnetic || isSlidingHandle) && isOffsetFoil) targetDate10 = getAct(8);
        else if ((isMagnetic || isSlidingHandle) && isScreenPrint) targetDate10 = getAct(12);
        else if (isTopBottom && hasInner) targetDate10 = getAct(11);
        else if (isTopBottom || isSlidingBox) targetDate10 = getAct(8);
        if (targetDate10) plans[10] = addWorkdays(targetDate10, 2);
        const innercase1 = isTopBottom && hasInner; const innercase2 = isMagnetic && hasInner && I === 'Screen print'; const innercase3 = isMagnetic && hasInner && isOffsetFoil; const innercase4 = F === 'PVC Pad';
        if (innercase4) {
            plans[11] = addWorkdays(getAct(3), 1);
        } else if (innercase1) {
            plans[11] = addWorkdays(getAct(8), 1);
        } else if (innercase2) {
            plans[11] = addWorkdays(getAct(12), 1);
        } else if (innercase3) {
            plans[11] = addWorkdays(getAct(9), 1);
        }
        let targetDate12 = null;
        if (hasReadystock && I === 'Screen print') targetDate12 = getAct(2);
        else if (F === 'PVC Pad') targetDate12 = getAct(7);
        else if (isPaperBag && isScreenPrint) targetDate12 = getAct(8);
        else if ((isMagnetic || isSlidingHandle) && I === 'Screen print') targetDate12 = getAct(9);
        else if ((isMagnetic && hasInner) && I === 'Screen print') targetDate12 = getAct(9);
        else if ((isTopBottom && hasInner) && I === 'Screen print') targetDate12 = getAct(10);
        else if ((isTopBottom || isSlidingBox) && I === 'Screen print') targetDate12 = getAct(10);
        if (targetDate12) plans[12] = addWorkdays(targetDate12, 1);
        const isboxtypecon = (F === 'Cards' || F === 'Hooks');
        if (hasReadystock && I === 'No') plans[13] = addWorkdays(getAct(2), 1);
        else if (isboxtypecon) plans[13] = addWorkdays(getAct(2), 1);
        else if (F === 'Foam' && getAct(5)) plans[13] = addWorkdays(getAct(5), 1);
        else if (F === 'Paper Box' && getAct(10)) plans[13] = addWorkdays(getAct(10), 1);
        else if (isPaperBag && isScreenPrint) plans[13] = addWorkdays(getAct(10), 1);
        else if (isPaperBag && isOffsetFoil) plans[13] = addWorkdays(getAct(10), 1);
        else if (isMagnetic && hasInner && isOffsetFoil) plans[13] = addWorkdays(getAct(10), 1);
        else if ((isMagnetic || isSlidingHandle) && isOffsetFoil) plans[13] = addWorkdays(getAct(10), 1);
        else if ((isMagnetic || isSlidingHandle) && isScreenPrint) plans[13] = addWorkdays(getAct(10), 1);
        else if (isTopBottom && hasInner && isOffsetFoil) plans[13] = addWorkdays(getAct(10), 1);
        else if (F === 'PVC Pad' && getAct(12)) plans[13] = addWorkdays(getAct(12), 1);
        else if (isTopBottom && hasInner && isScreenPrint) plans[13] = addWorkdays(getAct(12), 1);
        else if ((isTopBottom || isSlidingBox) && isOffsetFoil) plans[13] = addWorkdays(getAct(10), 1);
        else if ((isTopBottom || isSlidingBox) && isScreenPrint) plans[13] = addWorkdays(getAct(12), 1);
        if (getAct(13)) plans[14] = addWorkdays(getAct(13), 1);
        if (getAct(14)) plans[15] = addWorkdays(getAct(14), 1);
        if (N) plans[16] = N;
        // --- PRODUCTION LOGIC END ---

        // --- 3. GENERATE UPDATE DATA (FIXED FOR HOLD) ---
        for (let s = 1; s <= 16; s++) {
            const key = `${jobId}_${s}`;
            const existingTask = taskMap[key];

            if (plans[s]) {
                const sqlDate = plans[s].format('YYYY-MM-DD HH:mm:ss');

                // If the job was already on Hold in the DB, KEEP IT on Hold.
                const currentStatus = (existingTask && existingTask.status === 'Hold') ? 'Hold' : 'Pending';

                taskUpdates.push([jobId, s, sqlDate, currentStatus]);
            } else if (existingTask && !existingTask.actual) {
                tasksToDelete.push(existingTask.id);
            }
        }
    }

    // 4. DATABASE UPDATES
    if (tasksToDelete.length > 0) await db.query("DELETE FROM fms_dibiaa_tasks WHERE id IN (?)", [tasksToDelete]);

    if (taskUpdates.length > 0) {
        // --- CRITICAL CHANGE 2: SQL 'IF' Logic to protect Hold and Completed status ---
        const taskSql = `
    INSERT INTO fms_dibiaa_tasks (job_id, step_id, plan_date, status) 
    VALUES ? 
    ON DUPLICATE KEY UPDATE 
    plan_date = IF(status = 'Completed', plan_date, VALUES(plan_date)),
    status = IF(status = 'Hold' OR status = 'Completed', status, VALUES(status)),
    hold_reason = hold_reason,
    hold_timestamp = hold_timestamp,
    unhold_timestamp = unhold_timestamp`;

        await db.query(taskSql, [taskUpdates]);
    }
    return true;
};

app.post('/fms/restore-logs', upload.single('file'), async (req, res) => {
    if (!req.file) return res.status(400).json({ message: "No file uploaded" });

    try {
        const results = [];
        fs.createReadStream(req.file.path)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', async () => {
                const [jobs] = await db.query("SELECT job_id, job_number FROM fms_dibiaa_raw");
                const jobMap = {};
                jobs.forEach(j => { if (j.job_number) jobMap[j.job_number.toString()] = j.job_id; });

                const updateRows = [];
                for (const row of results) {
                    const rawJobNo = row.job_number || row['job_number'];
                    const jobId = rawJobNo ? jobMap[rawJobNo.toString().trim()] : null;

                    if (jobId) {
                        let formattedDate = null;
                        if (row.actual_date) {
                            const dateObj = dayjs(row.actual_date);
                            if (dateObj.isValid()) formattedDate = dateObj.format('YYYY-MM-DD HH:mm:ss');
                        }

                        // Determine status based on presence of actual date
                        const rowStatus = (formattedDate && formattedDate !== "") ? 'Completed' : 'Pending';

                        updateRows.push([
                            jobId,
                            parseInt(row.step_id),
                            formattedDate,
                            row.delay_hours || 0,
                            row.delay_reason || '',
                            row.contractor_printer || row['Contractor/Printer'] || '',
                            row.quantity || 0,
                            rowStatus
                        ]);
                    }
                }

                if (updateRows.length > 0) {
                    // 1. Restore the Actual Dates and mark as Completed
                    const sql = `INSERT INTO fms_dibiaa_tasks 
                                 (job_id, step_id, actual_date, delay_hours, delay_reason, custom_field_1, custom_field_2, status) 
                                 VALUES ? ON DUPLICATE KEY UPDATE 
                                 actual_date = VALUES(actual_date), 
                                 status = VALUES(status), 
                                 delay_hours = VALUES(delay_hours), 
                                 delay_reason = VALUES(delay_reason), 
                                 custom_field_1 = VALUES(custom_field_1), 
                                 custom_field_2 = VALUES(custom_field_2)`;
                    await db.query(sql, [updateRows]);

                    // 2. TRIGGER THE RECALCULATION
                    // This function now uses those Actual Dates to build the correct Plan Dates
                    await performFmsSync();

                    fs.unlinkSync(req.file.path);
                    res.json({ message: `Success: Restored ${updateRows.length} entries and recalculated Plan Dates for all steps.` });
                } else {
                    if (fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
                    res.status(400).json({ message: "No matching data found." });
                }
            });
    } catch (e) {
        if (req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
        res.status(500).json({ error: e.message });
    }
});

app.get('/fms/download-sample-csv', (req, res) => {
    const headers = "job_number,step_id,actual_date,delay_hours,delay_reason,contractor_printer,quantity,status\n";
    // Now showing the exact format: YYYY-MM-DD HH:mm:ss
    const sampleRow = "14503,8,2026-02-23 15:38:46,2,Machine Down,Shahjad ji,500,Completed";

    const csvContent = headers + sampleRow;
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=fms_restore_sample.csv');
    res.status(200).send(csvContent);
});

app.post('/fms/pc-summary', async (req, res) => {
    try {
        const { clients, steps, jobNumbers, statuses } = req.body;

        // 1. New params array (No dates needed now)
        let params = [];

        // 2. Base WHERE clause: Shows everything where plan exists but actual is blank
        let whereClause = "WHERE t.plan_date IS NOT NULL AND (t.actual_date IS NULL OR t.actual_date = '')";

        // 3. Multi-select Filter logic
        if (clients && clients.length > 0) {
            whereClause += " AND r.company_name IN (?)";
            params.push(clients);
        }
        if (steps && steps.length > 0) {
            whereClause += " AND s.step_name IN (?)";
            params.push(steps);
        }
        if (jobNumbers && jobNumbers.length > 0) {
            whereClause += " AND r.job_number IN (?)";
            params.push(jobNumbers);
        }

        const sql = `
            SELECT 
                r.job_number, r.order_by, t.plan_date, 
                r.company_name, s.step_name, r.box_type, 
                r.box_style, r.quantity, r.city
            FROM fms_dibiaa_tasks t
            JOIN fms_dibiaa_raw r ON t.job_id = r.job_id
            JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id
            ${whereClause}
            ORDER BY t.plan_date ASC`;

        const [rows] = await db.query(sql, params);

        // 4. Status Filtering
        const filteredRows = rows.filter(row => {
            const isPast = dayjs().isAfter(dayjs(row.plan_date));
            const status = isPast ? 'Pending' : 'Upcoming';
            return (statuses && statuses.length > 0) ? statuses.includes(status) : true;
        });

        const stats = {
            totalQty: filteredRows.reduce((acc, curr) => acc + (Number(curr.quantity) || 0), 0),
            totalJobs: filteredRows.length,
            uniqueClients: new Set(filteredRows.map(r => r.company_name)).size
        };

        res.json({ data: filteredRows, stats });

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/mis/checklist-report', async (req, res) => {
    const { start, end } = req.query;
    const today = dayjs().format('YYYY-MM-DD');

    const sql = `
        SELECT 
            MAX(TRIM(employee_name)) as employee_name, -- Takes the longest/most recent name
            LOWER(TRIM(employee_email)) as employee_email,
            COUNT(id) as total_task,
            SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) as total_pending,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as total_completed,
            SUM(CASE 
                WHEN (status = 'Pending' AND target_date < ?) OR (status = 'Completed' AND DATE(completed_at) > target_date) 
                THEN 1 ELSE 0 
            END) as total_delayed
        FROM checklist_tasks
        WHERE target_date BETWEEN ? AND ?
        GROUP BY LOWER(TRIM(employee_email)) -- Strictly unique grouping
        HAVING total_task > 0`;

    try {
        const [rows] = await db.query(sql, [today, start, end]);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// 1. Fetch All Logs for Admin
app.get('/fms/logs', async (req, res) => {
    try {
        const sql = `
            SELECT 
                t.*, 
                r.job_number, 
                r.quantity, 
                s.step_name
            FROM fms_dibiaa_tasks t
            LEFT JOIN fms_dibiaa_raw r ON t.job_id = r.job_id
            LEFT JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id
            ORDER BY t.actual_date DESC`;
            
        const [rows] = await db.query(sql);
        res.json(rows);
    } catch (e) { 
        console.error("SQL Error:", e.message); 
        res.status(500).json({ error: e.message }); 
    }
});

app.get('/fms/contractor-logs', async (req, res) => {
    try {
        const sql = `
            SELECT 
                t_work.id, 
                t_assign.custom_field_1 AS contractor_name, -- Pulled from Step 8
                r.job_number, 
                r.quantity, 
                s.step_name, 
                t_work.plan_date, 
                t_work.actual_date, 
                t_work.step_id
            FROM fms_dibiaa_tasks t_work
            /* Join with Step 8 to get the name */
            INNER JOIN fms_dibiaa_tasks t_assign 
                ON t_work.job_id = t_assign.job_id 
                AND t_assign.step_id = 8 
            INNER JOIN fms_dibiaa_raw r ON t_work.job_id = r.job_id
            INNER JOIN fms_dibiaa_steps_config s ON t_work.step_id = s.step_id
            /* Focus only on the work steps */
            WHERE t_work.step_id IN (9, 10)
              AND t_assign.custom_field_1 IS NOT NULL 
              AND t_assign.custom_field_1 != '---'
            ORDER BY t_work.plan_date DESC`;
            
        const [rows] = await db.query(sql);
        res.json(rows);
    } catch (e) { 
        res.status(500).json({ error: e.message }); 
    }
});

app.get('/fms/printer-logs', async (req, res) => {
    try {
        const sql = `
            SELECT 
                t.id, 
                t.custom_field_1 AS printer_name, 
                t.custom_field_2 AS step_qty, -- The actual production count
                r.job_number, 
                s.step_name, 
                t.plan_date, 
                t.actual_date, 
                t.step_id
            FROM fms_dibiaa_tasks t
            LEFT JOIN fms_dibiaa_raw r ON t.job_id = r.job_id
            LEFT JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id
            WHERE t.step_id IN (11, 12) 
              AND t.actual_date IS NOT NULL
              AND t.custom_field_1 IS NOT NULL 
              AND t.custom_field_1 != '---'
            ORDER BY t.actual_date DESC`;
            
        const [rows] = await db.query(sql);
        res.json(rows);
    } catch (e) { 
        res.status(500).json({ error: e.message }); 
    }
});

// 1. CLEAR LOG: Sets actual_date to NULL and status back to Pending
app.put('/fms/clear-log', async (req, res) => {
    const { id } = req.body;
    try {
        await db.query(
            "UPDATE fms_dibiaa_tasks SET actual_date = NULL, status = 'Pending' WHERE id = ?",
            [id]
        );
        res.json({ message: "Log cleared successfully" });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// 2. DELETE TASK: Completely removes the task from the database
app.delete('/fms/delete-task/:id', async (req, res) => {
    const { id } = req.params;
    try {
        await db.query("DELETE FROM fms_dibiaa_tasks WHERE id = ?", [id]);
        res.json({ message: "Task deleted successfully" });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// 2. Update Actual Date Manually
app.put('/fms/update-actual', async (req, res) => {
    const { id, actual_date } = req.body;
    try {
        await db.query("UPDATE fms_dibiaa_tasks SET actual_date = ?, status = ? WHERE id = ?",
            [actual_date || null, actual_date ? 'Completed' : 'Pending', id]);
        res.json({ message: "Updated Successfully" });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// 1. Transfer Checklist Task
app.put('/checklist/transfer', async (req, res) => {
    const { id, new_email, new_date } = req.body;
    try {
        // Fetch the new user's name first
        const [u] = await db.query("SELECT name FROM users WHERE email = ?", [new_email]);
        const new_name = u.length > 0 ? u[0].name : new_email;

        await db.query(
            "UPDATE checklist_tasks SET employee_email = ?, employee_name = ?, target_date = ? WHERE id = ?",
            [new_email, new_name, new_date, id]
        );
        res.json({ message: "Task Transferred Successfully" });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// 2. Fetch Checklist Comments
app.get('/checklist/comments/:id', async (req, res) => {
    const sql = `
        SELECT 
            user_name, 
            comment, 
            DATE_FORMAT(created_at, '%d/%m/%Y %H:%i:%s') as formatted_date 
        FROM checklist_comments 
        WHERE checklist_id = ? 
        ORDER BY created_at DESC`;

    const [r] = await db.query(sql, [req.params.id]);
    res.json(r);
});
// 3. Post Checklist Comment
app.post('/checklist/comments', async (req, res) => {
    await db.query(
        "INSERT INTO checklist_comments (checklist_id, user_name, comment) VALUES (?,?,?)",
        [req.body.checklist_id, req.body.user_name, req.body.comment]
    );
    res.json({ message: "Comment Added" });
});

// Bulk Transfer Checklist Tasks for a specific date
app.put('/checklist/bulk-transfer', async (req, res) => {
    const { current_email, current_date, new_email, new_date } = req.body;
    try {
        const [u] = await db.query("SELECT name FROM users WHERE email = ?", [new_email]);
        const new_name = u.length > 0 ? u[0].name : new_email;

        const sql = `
            UPDATE checklist_tasks 
            SET employee_email = ?, employee_name = ?, target_date = ? 
            WHERE employee_email = ? AND target_date = ? AND status = 'Pending'`;

        const [result] = await db.query(sql, [new_email, new_name, new_date, current_email, current_date]);
        res.json({ message: `Successfully transferred ${result.affectedRows} tasks.` });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Bulk Delete Checklist Tasks for a specific doer on a specific date
app.delete('/checklist/bulk-delete', async (req, res) => {
    const { employee_email, target_date } = req.query;
    try {
        // This query is safe because it only targets 'Pending' status
        const sql = "DELETE FROM checklist_tasks WHERE employee_email = ? AND target_date = ? AND status = 'Pending'";
        const [result] = await db.query(sql, [employee_email, target_date]);

        res.json({ message: `Deleted ${result.affectedRows} tasks.` });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

//hi hello

// 3. Reset Job Logic
app.post('/fms/reset-job', async (req, res) => {
    const { job_number, reset_to_step_id } = req.body;
    try {
        const [job] = await db.query("SELECT job_id FROM fms_dibiaa_raw WHERE job_number = ?", [job_number]);
        if (job.length === 0) return res.status(404).json({ message: "Job Number not found" });
        const jobId = job[0].job_id;

        // THE FIX: Only clear the actual_date for the chosen step and higher.
        // If you reset to Step 1, Step 4 remains 'Completed' in the DB.
        await db.query(`
            UPDATE fms_dibiaa_tasks 
            SET actual_date = NULL, status = 'Pending' 
            WHERE job_id = ? AND step_id >= ? AND step_id != 16`,
            [jobId, reset_to_step_id]
        );

        res.json({ message: "Reset successful" });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Route to Toggle Hold/Unhold status
app.post('/fms/toggle-hold', async (req, res) => {
    const { job_number, action, reason } = req.body;
    const newStatus = action === 'Hold' ? 'Hold' : 'Pending';
    
    // FORCE IST TIMEZONE
    const nowIST = dayjs().tz("Asia/Kolkata").format('YYYY-MM-DD HH:mm:ss');

    try {
        let sql = "";
        let params = [];

        if (action === 'Hold') {
            sql = `
                UPDATE fms_dibiaa_tasks t
                JOIN fms_dibiaa_raw r ON t.job_id = r.job_id
                SET t.status = ?, t.hold_reason = ?, t.hold_timestamp = ?
                WHERE r.job_number = ? AND t.actual_date IS NULL
            `;
            params = [newStatus, reason, nowIST, job_number];
        } else {
            sql = `
                UPDATE fms_dibiaa_tasks t
                JOIN fms_dibiaa_raw r ON t.job_id = r.job_id
                SET t.status = ?, t.unhold_timestamp = ?
                WHERE r.job_number = ? AND t.actual_date IS NULL
            `;
            params = [newStatus, nowIST, job_number];
        }

        await db.query(sql, params);
        res.json({ message: `Job ${job_number} is now ${newStatus} at IST ${nowIST}` });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Endpoint for Checklist Detail Popup
app.get('/mis/checklist-tasks', async (req, res) => {
    const { email, start, end } = req.query;
    const [rows] = await db.query(
        "SELECT description, target_date, status, completed_at FROM checklist_tasks WHERE employee_email=? AND target_date BETWEEN ? AND ? ORDER BY target_date",
        [email, start, end]
    );
    res.json(rows);
});

app.get('/fms/dibiaa-config', async (req, res) => { const [r] = await db.query("SELECT * FROM fms_dibiaa_steps_config"); res.json(r); });
app.post('/fms/dibiaa-config', async (req, res) => { const { step_id, doer_emails, visible_columns } = req.body; await db.query("UPDATE fms_dibiaa_steps_config SET doer_emails=?, visible_columns=? WHERE step_id=?", [doer_emails, visible_columns, step_id]); res.json({ message: "Saved" }); });

// --- DEPLOYMENT CONFIG (VERCEL) ---
app.use(express.static(path.join(__dirname, 'build')));
app.get(/.*/, (req, res) => {
    res.sendFile(path.join(__dirname, 'build', 'index.html'));
});

// --- SERVER STARTUP ---
const PORT = process.env.PORT || 8800;
if (require.main === module) {
    app.listen(PORT, () => {
        console.log(`Server running on port ${PORT}`);
    });
}

module.exports = app;
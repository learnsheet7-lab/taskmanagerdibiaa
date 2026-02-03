require('dotenv').config();
const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const dayjs = require('dayjs'); 
const customParseFormat = require('dayjs/plugin/customParseFormat');
dayjs.extend(customParseFormat);
const { google } = require('googleapis'); 
const fs = require('fs');
const path = require('path'); // Add this line
const multer = require('multer');
const csv = require('csv-parser');

const app = express();
app.use(express.json());
app.use(cors());
// To this:
const upload = multer({ dest: '/tmp' });

// --- DATABASE CONNECTION (ENV VARIABLES) ---
const db = mysql.createPool({
    host: process.env.DB_HOST, 
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    enableKeepAlive: true, // Crucial for Vercel
    keepAliveInitialDelay: 0,
    dateStrings: true  // <--- ADD THIS LINE HERE
});

// --- GOOGLE AUTH (ENV OR FILE) ---
const getAuth = () => {
    // Priority 1: Environment Variable (For Vercel/Production)
    if (process.env.GOOGLE_CREDENTIALS) {
        try {
            const credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS);
            return new google.auth.GoogleAuth({ credentials, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
        } catch (e) {
            console.error("Error parsing GOOGLE_CREDENTIALS env var", e);
        }
    }
    // Priority 2: Local File (For Local Development)
    if (fs.existsSync('credentials.json')) {
        return new google.auth.GoogleAuth({ keyFile: 'credentials.json', scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    }
    throw new Error("Google Credentials not found in ENV or File");
};

// HELPERS
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
    if (!d) return null;
    const dt = dayjs(d.toString().trim(), ['DD/MM/YYYY HH:mm:ss', 'YYYY-MM-DD HH:mm:ss', 'DD/MM/YYYY', 'YYYY-MM-DD'], true);
    return dt.isValid() ? dt : null;
};
const addWorkdays = (startDate, days) => {
    if (!startDate) return null;
    let d = dayjs(startDate);
    if (!d.isValid()) return null;
    let added = 0;
    while (added < Math.floor(days)) {
        d = d.add(1, 'day');
        if (d.day() !== 0) added++; 
    }
    if (days % 1 !== 0) d = d.add((days % 1) * 24, 'hour');
    if (d.day() === 0) d = d.add(1, 'day').hour(9).minute(0).second(0);
    if (d.hour() < 9) d = d.hour(9);
    if (d.hour() >= 18) d = d.add(1, 'day').hour(9);
    return d;
};

// --- AUTH & USERS ---
app.post('/login', async (req, res) => { try { const [r] = await db.query("SELECT * FROM users WHERE (email = ? OR mobile = ?) AND password = ?", [req.body.identifier, req.body.identifier, req.body.password]); res.json(r.length ? r[0] : { message: "User not found" }); } catch (e) { res.status(500).json(e); } });
app.get('/users', async (req, res) => { const [r] = await db.query("SELECT * FROM users"); res.json(r); });
app.post('/users', async (req, res) => { await db.query("INSERT INTO users (name, role, department, email, mobile, password) VALUES (?,?,?,?,?,?)", [req.body.name, req.body.role, req.body.department, req.body.email, req.body.mobile, req.body.password]); res.json({message:"Created"}); });
app.put('/users/update', async (req, res) => { const { name, email, role, department, mobile, id, password } = req.body; if(password && password.trim() !== "") await db.query("UPDATE users SET name=?, email=?, role=?, department=?, mobile=?, password=? WHERE id=?", [name, email, role, department, mobile, password, id]); else await db.query("UPDATE users SET name=?, email=?, role=?, department=?, mobile=? WHERE id=?", [name, email, role, department, mobile, id]); res.json({message:"Updated"}); });
app.delete('/users/:id', async (req, res) => { await db.query("DELETE FROM users WHERE id=?", [req.params.id]); res.json({message: "User Deleted"}); });
app.post('/users/change-password-secure', async (req, res) => { const { id, currentPassword, newPassword } = req.body; const [u] = await db.query("SELECT * FROM users WHERE id=? AND password=?", [id, currentPassword]); if(u.length === 0) return res.status(401).json({message: "Current password incorrect"}); await db.query("UPDATE users SET password=? WHERE id=?", [newPassword, id]); res.json({message: "Password Changed"}); });

// --- DASHBOARD ---
app.get('/dashboard/:email/:role', async (req, res) => { 
    const {email,role}=req.params; const today = dayjs().format('YYYY-MM-DD');

    const base=role==='Admin'?"":`WHERE assigned_to_email='${email}'`; const and=role==='Admin'?"WHERE":"AND";
    const [delPending]=await db.query(`SELECT COUNT(*) c FROM tasks ${base} ${and} status IN ('Pending','Revision Requested','Waiting Approval')`);
    const [delRevised]=await db.query(`SELECT COUNT(*) c FROM tasks ${base} ${and} status IN ('Revised', 'Revision Requested')`);
    const [delCompleted]=await db.query(`SELECT COUNT(*) c FROM tasks ${base} ${and} status='Completed'`);
    const [delToday]=await db.query(`SELECT * FROM tasks ${base} ${and} target_date <= '${today}' AND status!='Completed' ORDER BY target_date ASC`);
    
    const chkBase=role==='Admin'?"":`WHERE employee_email='${email}'`; const chkAnd=role==='Admin'?"WHERE":"AND";
    const [chkTotal]=await db.query(`SELECT COUNT(*) c FROM checklist_tasks ${chkBase} ${chkAnd} target_date <= '${today}'`);
    const [chkPending]=await db.query(`SELECT COUNT(*) c FROM checklist_tasks ${chkBase} ${chkAnd} target_date <= '${today}' AND status='Pending'`);
    const [chkCompleted]=await db.query(`SELECT COUNT(*) c FROM checklist_tasks ${chkBase} ${chkAnd} target_date <= '${today}' AND status='Completed'`);
    const [chkTodayTasks]=await db.query(`SELECT * FROM checklist_tasks ${chkBase} ${chkAnd} target_date <= '${today}' AND status='Pending' ORDER BY target_date ASC`);

    let fmsBase = "SELECT t.*, r.job_number, r.company_name FROM fms_dibiaa_tasks t JOIN fms_dibiaa_raw r ON t.job_id=r.job_id JOIN fms_dibiaa_steps_config s ON t.step_id=s.step_id WHERE 1=1";
    if(role !== 'Admin') { const [mySteps] = await db.query("SELECT step_id FROM fms_dibiaa_steps_config WHERE doer_emails LIKE ?", [`%${email}%`]); const ids = mySteps.map(s=>s.step_id).join(',') || '0'; fmsBase += ` AND t.step_id IN (${ids})`; }
    const [fmsAll] = await db.query(fmsBase);
    const fmsTotal = fmsAll.length; const fmsPending = fmsAll.filter(t=>t.status==='Pending').length; const fmsCompleted = fmsAll.filter(t=>t.status==='Completed').length;
    const fmsToday = fmsAll.filter(t => t.status === 'Pending' && dayjs(t.plan_date).isBefore(dayjs().endOf('day')));

    res.json({
        delegation: { pending: delPending[0].c, revised: delRevised[0].c, completed: delCompleted[0].c, today: delToday },
        checklist: { pending: chkPending[0].c, total: chkTotal[0].c, completed: chkCompleted[0].c, today: chkTodayTasks },
        fms: { pending: fmsPending, total: fmsTotal, completed: fmsCompleted, today: fmsToday }
    }); 
});

// --- MODULES ---
app.post('/checklist', async (req, res) => { const { description, employee_email, employee_name, frequency, start_date } = req.body; const [h] = await db.query("SELECT holiday_date FROM holidays"); const holidays = new Set(h.map(x=>x.holiday_date)); const tasks=[]; let c=dayjs(start_date); const end=dayjs().endOf('year'); while(c.isBefore(end)||c.isSame(end,'day')){const d=c.format('YYYY-MM-DD'); if((c.day()!==0||d===start_date)&&!holidays.has(d)) tasks.push(['CHK-'+Math.floor(Math.random()*1000),description,employee_email,employee_name,frequency,d,'Pending']); if(frequency==='Daily')c=c.add(1,'day');else if(frequency==='Weekly')c=c.add(1,'week');else if(frequency==='Monthly')c=c.add(1,'month');else if(frequency==='Quarterly')c=c.add(3,'month');else c=c.add(1,'year');} if(tasks.length>0){await db.query("INSERT INTO checklist_tasks (uid,description,employee_email,employee_name,frequency,target_date,status) VALUES ?", [tasks]); res.json({message:`Generated ${tasks.length}`});} else res.json({message:"No dates"}); });
app.get('/checklist/:email/:role', async (req, res) => { const today=dayjs().format('YYYY-MM-DD'); const q=req.params.role==='Admin'?"SELECT * FROM checklist_tasks WHERE (target_date <= ? AND status='Pending') OR target_date = ? ORDER BY target_date ASC":"SELECT * FROM checklist_tasks WHERE employee_email=? AND ((target_date<=? AND status='Pending') OR target_date=?) ORDER BY target_date ASC"; const [r]=await db.query(q,req.params.role==='Admin'?[today,today]:[req.params.email,today,today]); res.json(r); });
app.put('/checklist/complete', async (req, res) => { await db.query("UPDATE checklist_tasks SET status='Completed', completed_at=NOW() WHERE id=?", [req.body.id]); res.json({message:"Done"}); });
app.post('/tasks', async (req, res) => { await db.query("INSERT INTO tasks (task_uid,employee_name,assigned_to_email,approver_email,description,target_date,priority,approval_needed,assigned_by,remarks,status,previous_status) VALUES (?,?,?,?,?,?,?,?,?,?,'Pending','Pending')",['T-'+Math.floor(Math.random()*9000),req.body.employee_name,req.body.email,req.body.approver_email,req.body.description,req.body.target_date,req.body.priority,req.body.approval_needed,req.body.assigned_by||'System',req.body.remarks||'']); res.json({message:"Delegated"}); });
app.get('/tasks/:email/:role', async (req, res) => { const q=req.params.role==='Admin'?"SELECT * FROM tasks ORDER BY created_at DESC":"SELECT * FROM tasks WHERE assigned_to_email=? ORDER BY created_at DESC"; const [r]=await db.query(q,[req.params.email]); res.json(r); });
app.delete('/tasks/:id', async (req, res) => { await db.query("DELETE FROM tasks WHERE id=?", [req.params.id]); res.json({message:"Deleted"}); });
app.put('/tasks/update-status', async (req, res) => { const {id,status,revised_date,remarks,is_rejection}=req.body; if(is_rejection) await db.query("UPDATE tasks SET status = CASE WHEN previous_status IS NULL OR previous_status='' OR previous_status='Waiting Approval' THEN 'Pending' ELSE previous_status END WHERE id=?", [id]); else if(status==='Revision Requested') await db.query("UPDATE tasks SET previous_status=status, status=?, revised_date_request=?, revision_remarks=? WHERE id=?", [status,revised_date,remarks,id]); else if(status==='Revised') await db.query("UPDATE tasks SET status='Revised', target_date=revised_date_request WHERE id=?", [id]); else await db.query("UPDATE tasks SET previous_status=status, status=? WHERE id=?", [status,id]); res.json({message:"Updated"}); });
app.put('/tasks/edit/:id', async (req, res) => { const { description, target_date, priority, approval_needed, remarks, assigned_to_email, approver_email, employee_name } = req.body; await db.query("UPDATE tasks SET description=?, target_date=?, priority=?, approval_needed=?, remarks=?, assigned_to_email=?, approver_email=?, employee_name=? WHERE id=?", [description, target_date, priority, approval_needed, remarks, assigned_to_email, approver_email, employee_name, req.params.id]); res.json({ message: "Task Updated" }); });
app.get('/comments/:taskId', async (req, res) => { const [r]=await db.query("SELECT id,task_id,user_name,comment,DATE_FORMAT(created_at,'%d/%m/%Y %H:%i:%s') as formatted_date FROM task_comments WHERE task_id=? ORDER BY created_at DESC",[req.params.taskId]); res.json(r); });
app.post('/comments', async (req, res) => { await db.query("INSERT INTO task_comments (task_id,user_name,comment) VALUES (?,?,?)",[req.body.task_id,req.body.user_name,req.body.comment]); res.json({message:"Added"}); });
app.get('/approvals/:email', async (req, res) => { const [u] = await db.query("SELECT role FROM users WHERE email=?", [req.params.email]); let q = "SELECT * FROM tasks WHERE status IN ('Waiting Approval','Revision Requested')"; let params = []; if (u.length === 0 || u[0].role !== 'Admin') { q += " AND LOWER(approver_email)=LOWER(?)"; params.push(req.params.email); } const [r] = await db.query(q, params); res.json(r); });
app.get('/holidays', async (req, res) => { const [r] = await db.query("SELECT * FROM holidays ORDER BY holiday_date"); res.json(r); });
app.post('/holidays', async (req, res) => { await db.query("INSERT INTO holidays (holiday_date, name) VALUES (?,?)", [req.body.date, req.body.name]); res.json({message:"Added"}); });
app.get('/mis/tasks', async (req, res) => { const [rows]=await db.query("SELECT description,target_date,status,completed_at FROM tasks WHERE assigned_to_email=? AND target_date BETWEEN ? AND ? ORDER BY target_date",[req.query.email,req.query.start,req.query.end]); res.json(rows); });
app.post('/mis/plan', async (req, res) => { const {email,date,count}=req.body; const [ex]=await db.query("SELECT id FROM employee_plans WHERE employee_email=? AND plan_date=?",[email,date]); if(ex.length>0) await db.query("UPDATE employee_plans SET planned_count=? WHERE id=?",[count,ex[0].id]); else await db.query("INSERT INTO employee_plans (employee_email,plan_date,planned_count) VALUES (?,?,?)",[email,date,count]); res.json({message:"Saved"}); });
app.post('/tasks/upload', upload.single('file'), async (req, res) => { if (!req.file) return res.status(400).json({ message: "No file uploaded" }); try { const results = []; fs.createReadStream(req.file.path).pipe(csv()).on('data', (data) => results.push(data)).on('end', async () => { const [users] = await db.query("SELECT email, name FROM users"); const bulkTasks = []; const assignedBy = req.body.assigned_by || 'Admin Upload'; for (const row of results) { const empName = users.find(u => u.email === row.employee_email)?.name || row.employee_email; const tDate = parseToMySQLDate(row.target_date); if(tDate) { bulkTasks.push(['T-'+Math.floor(Math.random()*90000), empName, row.employee_email, row.approver_email, row.description, tDate, row.priority || 'Medium', row.approval_needed || 'No', assignedBy, row.remarks || '', 'Pending', 'Pending']); } } if (bulkTasks.length > 0) { await db.query("INSERT INTO tasks (task_uid,employee_name,assigned_to_email,approver_email,description,target_date,priority,approval_needed,assigned_by,remarks,status,previous_status) VALUES ?", [bulkTasks]); } fs.unlinkSync(req.file.path); res.json({ message: `Delegated ${bulkTasks.length} tasks.` }); }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/checklist/upload', upload.single('file'), async (req, res) => { if (!req.file) return res.status(400).json({ message: "No file uploaded" }); try { const results = []; fs.createReadStream(req.file.path).pipe(csv()).on('data', (data) => results.push(data)).on('end', async () => { const [h] = await db.query("SELECT holiday_date FROM holidays"); const holidays = new Set(h.map(x => x.holiday_date)); const [users] = await db.query("SELECT email, name FROM users"); const bulkTasks = []; for (const row of results) { const empEmail = row.employee_email; const empName = users.find(u => u.email === empEmail)?.name || empEmail; const startDate = parseToMySQLDate(row.start_date); if (startDate) { let c = dayjs(startDate); const end = dayjs().endOf('year'); while (c.isBefore(end) || c.isSame(end, 'day')) { const d = c.format('YYYY-MM-DD'); if ((c.day() !== 0 || d === startDate) && !holidays.has(d)) { bulkTasks.push(['CHK-' + Math.floor(Math.random() * 100000), row.description, empEmail, empName, row.frequency, d, 'Pending']); } if (row.frequency === 'Daily') c = c.add(1, 'day'); else if (row.frequency === 'Weekly') c = c.add(1, 'week'); else if (row.frequency === 'Monthly') c = c.add(1, 'month'); else if (row.frequency === 'Quarterly') c = c.add(3, 'month'); else c = c.add(1, 'year'); } } } if (bulkTasks.length > 0) { await db.query("INSERT INTO checklist_tasks (uid,description,employee_email,employee_name,frequency,target_date,status) VALUES ?", [bulkTasks]); } fs.unlinkSync(req.file.path); res.json({ message: `Bulk Processed.` }); }); } catch (e) { res.status(500).json({ error: e.message }); } });

// --- MIS REPORT (FIXED PREVIOUS PLAN LOGIC) ---
app.get('/mis/report', async (req, res) => { 
    const {start,end}=req.query; 
    // Logic: 
    // 1. "planned": Fetches the *single most recent* plan set ON or BEFORE the 'end' date. (Not sum, just the last value).
    // 2. "total_task", "completed", etc.: Counts tasks strictly BETWEEN start and end dates.
    const sql=`
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
    const [rows]=await db.query(sql,[end, start, end]); 
    res.json(rows); 
});

// ================= DIBIAA FMS ENGINE (BULK OPTIMIZED) =================
app.post('/fms/sync-dibiaa', async (req, res) => {
    try {
        const SHEET_ID = '1C3qHR_jbjHgOQCM7MwRB4AZXtuY2W9jLpqIAEbYFWkQ';
        const auth = getAuth();
        if(!auth) return res.status(500).json({error: "Google Auth Failed"});

        const sheets = google.sheets({ version: 'v4', auth });
        const response = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `fmstask!A2:O` });
        const rows = response.data.values;
        if(!rows || rows.length === 0) return res.json({message: "No data found"});

        // 1. Bulk Insert Raw
        const rawValues = [];
        for(let i=0; i<rows.length; i++) {
            const r = rows[i];
            const rowIndex = 2 + i;
            rawValues.push([
                rowIndex, parseToMySQLDateTime(r[0]), r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], parseToMySQLDate(r[13]), r[14]
            ]);
        }
        if(rawValues.length > 0) {
            const rawSql = `INSERT INTO fms_dibiaa_raw (sheet_row_index, timestamp, otd_type, job_number, order_by, company_name, box_type, box_style, box_color, printing_type, printing_color, specification, city, quantity, lead_time, repeat_new) VALUES ? ON DUPLICATE KEY UPDATE otd_type=VALUES(otd_type), box_type=VALUES(box_type), printing_type=VALUES(printing_type), quantity=VALUES(quantity)`;
            await db.query(rawSql, [rawValues]);
        }

        // 2. Fetch Maps
        const [jobs] = await db.query("SELECT job_id, sheet_row_index FROM fms_dibiaa_raw");
        const jobMap = {}; jobs.forEach(j => jobMap[j.sheet_row_index] = j.job_id);
        const [allTasks] = await db.query("SELECT job_id, step_id, actual_date FROM fms_dibiaa_tasks");
        const taskMap = {}; allTasks.forEach(t => taskMap[`${t.job_id}_${t.step_id}`] = t.actual_date ? dayjs(t.actual_date) : null);

        // 3. Logic Engine
        const taskValues = [];
        for (let i = 0; i < rows.length; i++) {
            const r = rows[i]; const rowIndex = 2 + i; const jobId = jobMap[rowIndex];
            if(!jobId) continue;
            const getAct = (s) => taskMap[`${jobId}_${s}`];
            const A = parseDate(r[0]); const B = r[1]; const F = r[5]; const G = r[6]; const I = r[8]; const K = r[10]; const N = parseDate(r[13]);
            const hasInner = (K || '').toLowerCase().includes('inner'); const isOffsetFoil = (I === 'Offset Print' || I === 'Foil Print'); const isScreenPrint = (I === 'Screen print');
            
            let plans = {}; 
            if (I !== 'No' && A) plans[4] = addWorkdays(A, 3); //step 4 condition
            const step4Act = getAct(4);
            if ((B==='OTD' || B==='Jewellery (OTD)')) { if (I !== 'No' && step4Act) plans[1] = addWorkdays(step4Act, 6); else if (I === 'No' && A) plans[1] = addWorkdays(A, 6); } //step1 plan
            const step1Act = getAct(1); 
            if ((B==='OTD' || B==='Jewellery (OTD)')) { if (I !== 'No' && step1Act) plans[2] = addWorkdays(step1Act, 1); else if (I === 'No' && A && B!=='OTD' || B!=='Jewellery (OTD)') plans[2] = addWorkdays(A, 1); else if (step4Act) plans[2] = addWorkdays(step4Act, 1);} //step2 plan
            if (getAct(2)) plans[3] = addWorkdays(getAct(2), 1); //step3 plan
            if (!(F==='Paper Bag' || (F||'').endsWith('Tray'))) { if (getAct(2)) plans[5] = addWorkdays(getAct(2), 3); } //step5 plan
            if (I === 'Foil Print' && getAct(3)) plans[6] = addWorkdays(getAct(3), 3); //step6 plan
            if (I !== 'Foil Print' && getAct(3)) plans[7] = addWorkdays(getAct(3), 3); else if (getAct(6)) plans[7] = addWorkdays(getAct(6), 3); //step7 plan
            if (getAct(7)) plans[8] = addWorkdays(getAct(7), 1); //step8 plan
            if (I === 'Screen print') { const condition = (G==='Magnetic' || F==='Paper Bag' || (G||'').startsWith('Sliding Handle')) || (G==='Magnetic' && isOffsetFoil && hasInner) || (G==='Magnetic' && hasInner); if(condition && getAct(8)) plans[9] = addWorkdays(getAct(8), 1); }
            const isTopBottom = G==='Top-Bottom'; const isSlidingBox = G==='Sliding Box'; const isMagnetic = G==='Magnetic'; const isSlidingHandle = G==='Sliding Handle Box'; const isPaperBag = F==='Paper Bag'; let targetDate10 = null; if (isPaperBag && isScreenPrint) targetDate10 = getAct(12); else if (isPaperBag && isOffsetFoil) targetDate10 = getAct(8); else if (isMagnetic && hasInner) targetDate10 = getAct(11); else if ((isMagnetic || isSlidingHandle) && isOffsetFoil) targetDate10 = getAct(8); else if (isMagnetic || isSlidingHandle) targetDate10 = getAct(12); else if (isTopBottom && hasInner) targetDate10 = getAct(11); else if (isTopBottom || isSlidingBox) targetDate10 = getAct(8); if (targetDate10) plans[10] = addWorkdays(targetDate10, 2);
            const base11 = getAct(10) || getAct(9) || getAct(8); if (base11 && hasInner) plans[11] = addWorkdays(base11, 1);
            if (I === 'Screen print' && getAct(8)) plans[12] = addWorkdays(getAct(8), 1);
            const base13 = getAct(12) || getAct(11) || getAct(10); if (base13) plans[13] = addWorkdays(base13, 1);
            if (getAct(13)) plans[14] = addWorkdays(getAct(13), 1); if (getAct(14)) plans[15] = addWorkdays(getAct(14), 1); if (N) plans[16] = dayjs(N);

            for (let s = 1; s <= 16; s++) { if (plans[s]) { const sqlDate = plans[s].isValid() ? plans[s].format('YYYY-MM-DD HH:mm:ss') : null; if(sqlDate) taskValues.push([jobId, s, sqlDate, 'Pending']); } }
        }

        // 4. Bulk Insert Tasks
        if(taskValues.length > 0) {
            const taskSql = `INSERT INTO fms_dibiaa_tasks (job_id, step_id, plan_date, status) VALUES ? ON DUPLICATE KEY UPDATE plan_date=VALUES(plan_date)`;
            await db.query(taskSql, [taskValues]);
        }
        res.json({message: `Sync Complete. Processed ${rows.length} rows.`});
    } catch(e) { console.error("Sync Error:", e); res.status(500).json({error: e.message}); }
});

app.get('/fms/dibiaa-tasks', async (req, res) => { const { email, role } = req.query; const [configs] = await db.query("SELECT * FROM fms_dibiaa_steps_config"); const relevantSteps = role === 'Admin' ? configs : configs.filter(c => c.doer_emails && c.doer_emails.includes(email)); const stepIds = relevantSteps.map(s => s.step_id); if (stepIds.length === 0) return res.json({}); const [tasks] = await db.query(`SELECT t.*, r.job_number, r.company_name, r.box_type, r.quantity, r.quantity as total_qty, s.step_name, s.visible_columns, r.timestamp, r.otd_type, r.order_by, r.box_style, r.box_color, r.printing_type, r.printing_color, r.specification, r.city, r.lead_time, r.repeat_new FROM fms_dibiaa_tasks t JOIN fms_dibiaa_raw r ON t.job_id = r.job_id JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id WHERE t.status = 'Pending' AND t.step_id IN (?) ORDER BY t.plan_date ASC`, [stepIds]); const grouped = {}; relevantSteps.forEach(s => { const stepTasks = tasks.filter(t => t.step_id === s.step_id); if (stepTasks.length > 0) grouped[s.step_name] = stepTasks; }); res.json(grouped); });
app.post('/fms/dibiaa-complete', async (req, res) => { const { task_id, delay_reason, contractor, printer, qty } = req.body; const now = dayjs().format('YYYY-MM-DD HH:mm:ss'); const [t] = await db.query("SELECT plan_date FROM fms_dibiaa_tasks WHERE id=?", [task_id]); const plan = dayjs(t[0].plan_date); const delayHrs = dayjs().diff(plan, 'hour'); await db.query("UPDATE fms_dibiaa_tasks SET status='Completed', actual_date=?, delay_hours=?, delay_reason=?, custom_field_1=?, custom_field_2=? WHERE id=?", [now, delayHrs > 0 ? delayHrs : 0, delay_reason, contractor || printer, qty, task_id]); res.json({message: "Task Completed"}); });
app.get('/fms/dibiaa-config', async (req, res) => { const [r] = await db.query("SELECT * FROM fms_dibiaa_steps_config"); res.json(r); });
app.post('/fms/dibiaa-config', async (req, res) => { const { step_id, doer_emails, visible_columns } = req.body; await db.query("UPDATE fms_dibiaa_steps_config SET doer_emails=?, visible_columns=? WHERE step_id=?", [doer_emails, visible_columns, step_id]); res.json({message: "Saved"}); });

// =====================================================
// === DEPLOYMENT CONFIGURATION (VERCEL) ===
// =====================================================

// 1. Serve Static Files from React Build
app.use(express.static(path.join(__dirname, 'build')));

// 2. Handle Catch-All Routing (FIXED: Uses Regex to avoid Vercel 500 Error)
app.get(/.*/, (req, res) => {
  res.sendFile(path.join(__dirname, 'build', 'index.html'));
});

// =====================================================
// === SERVER STARTUP ===
// =====================================================

const PORT = process.env.PORT || 8800;

// Only start the server if running locally
// Vercel imports the app as a module, so it skips this block
if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}

// Export app for Vercel
module.exports = app;
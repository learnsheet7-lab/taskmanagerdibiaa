require('dotenv').config();
const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const dayjs = require('dayjs'); 
const customParseFormat = require('dayjs/plugin/customParseFormat');
const timezone = require('dayjs/plugin/timezone'); // Add this
const utc = require('dayjs/plugin/utc');           // Add this

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
// --- AUTH & USERS ---
app.post('/login', async (req, res) => { try { const [r] = await db.query("SELECT * FROM users WHERE (email = ? OR mobile = ?) AND password = ?", [req.body.identifier, req.body.identifier, req.body.password]); res.json(r.length ? r[0] : { message: "User not found" }); } catch (e) { res.status(500).json(e); } });
app.get('/users', async (req, res) => { const [r] = await db.query("SELECT * FROM users"); res.json(r); });
app.post('/users', async (req, res) => { await db.query("INSERT INTO users (name, role, department, email, mobile, password) VALUES (?,?,?,?,?,?)", [req.body.name, req.body.role, req.body.department, req.body.email, req.body.mobile, req.body.password]); res.json({message:"Created"}); });
app.put('/users/update', async (req, res) => { const { name, email, role, department, mobile, id, password } = req.body; if(password && password.trim() !== "") await db.query("UPDATE users SET name=?, email=?, role=?, department=?, mobile=?, password=? WHERE id=?", [name, email, role, department, mobile, password, id]); else await db.query("UPDATE users SET name=?, email=?, role=?, department=?, mobile=? WHERE id=?", [name, email, role, department, mobile, id]); res.json({message:"Updated"}); });
app.delete('/users/:id', async (req, res) => { await db.query("DELETE FROM users WHERE id=?", [req.params.id]); res.json({message: "User Deleted"}); });
app.post('/users/change-password-secure', async (req, res) => { const { id, currentPassword, newPassword } = req.body; const [u] = await db.query("SELECT * FROM users WHERE id=? AND password=?", [id, currentPassword]); if(u.length === 0) return res.status(401).json({message: "Current password incorrect"}); await db.query("UPDATE users SET password=? WHERE id=?", [newPassword, id]); res.json({message: "Password Changed"}); });

// --- DASHBOARD ---
// --- UPDATED DASHBOARD ROUTE ---
app.get('/dashboard/:email/:role', async (req, res) => { 
    const { email, role } = req.params;
    const { filterEmail } = req.query; // Admin selects from dropdown
    const today = dayjs().format('YYYY-MM-DD');

    // If Admin selects a user, target that user's email. Otherwise use standard logic.
    const targetEmail = (role === 'Admin' && filterEmail) ? filterEmail : email;
    
    // Determine if we are viewing "All" (Admin with no filter) or a specific person
    const isFiltered = (role === 'Admin' && !filterEmail) ? false : true;

    const base = !isFiltered ? "" : `WHERE assigned_to_email='${targetEmail}'`; 
    const and = !isFiltered ? "WHERE" : "AND";

    const [delPending] = await db.query(`SELECT COUNT(*) c FROM tasks ${base} ${and} status IN ('Pending','Revision Requested','Waiting Approval')`);
    const [delRevised] = await db.query(`SELECT COUNT(*) c FROM tasks ${base} ${and} status IN ('Revised', 'Revision Requested')`);
    const [delCompleted] = await db.query(`SELECT COUNT(*) c FROM tasks ${base} ${and} status='Completed'`);
    const [delToday] = await db.query(`SELECT * FROM tasks ${base} ${and} target_date <= '${today}' AND status!='Completed' ORDER BY target_date ASC`);
    
    const chkBase = !isFiltered ? "" : `WHERE employee_email='${targetEmail}'`; 
    const chkAnd = !isFiltered ? "WHERE" : "AND";
    const [chkTotal] = await db.query(`SELECT COUNT(*) c FROM checklist_tasks ${chkBase} ${chkAnd} target_date <= '${today}'`);
    const [chkPending] = await db.query(`SELECT COUNT(*) c FROM checklist_tasks ${chkBase} ${chkAnd} target_date <= '${today}' AND status='Pending'`);
    const [chkCompleted] = await db.query(`SELECT COUNT(*) c FROM checklist_tasks ${chkBase} ${chkAnd} target_date <= '${today}' AND status='Completed'`);
    
    // FMS Logic
    // Updated FMS Logic in /dashboard route
let fmsBase = "SELECT t.*, r.job_number, r.company_name, t.custom_field_1, t.custom_field_2 FROM fms_dibiaa_tasks t JOIN fms_dibiaa_raw r ON t.job_id=r.job_id WHERE 1=1";
    if(isFiltered) { 
        // Filter FMS by steps assigned to this user's email
        const [mySteps] = await db.query("SELECT step_id FROM fms_dibiaa_steps_config WHERE doer_emails LIKE ?", [`%${targetEmail}%`]); 
        const ids = mySteps.map(s=>s.step_id).join(',') || '0'; 
        fmsBase += ` AND t.step_id IN (${ids})`; 
    }
    const [fmsAll] = await db.query(fmsBase);

    res.json({
        delegation: { pending: delPending[0].c, revised: delRevised[0].c, completed: delCompleted[0].c, today: delToday },
        checklist: { pending: chkPending[0].c, total: chkTotal[0].c, completed: chkCompleted[0].c },
        fms: { pending: fmsAll.filter(t=>t.status==='Pending').length, total: fmsAll.length, completed: fmsAll.filter(t=>t.status==='Completed').length }
    }); 
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

app.put('/checklist/complete', async (req, res) => { await db.query("UPDATE checklist_tasks SET status='Completed', completed_at=NOW() WHERE id=?", [req.body.id]); res.json({message:"Done"}); });

// --- TASKS MODULE ---
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
// 2. FMS Status Summary Report - CORRECTED DELAY LOGIC
app.get('/fms/report-summary', async (req, res) => {
    const { start, end } = req.query;
    const sql = `
        SELECT 
            s.step_name,
            COUNT(t.id) as \`Total\`,
            SUM(CASE WHEN t.status = 'Pending' THEN 1 ELSE 0 END) as \`Pending\`,
            SUM(CASE WHEN t.status = 'Completed' THEN 1 ELSE 0 END) as \`Completed\`,
            SUM(CASE 
                WHEN t.status = 'Completed' AND t.actual_date > t.plan_date THEN 1 
                WHEN t.status = 'Pending' AND t.plan_date < NOW() THEN 1 
                ELSE 0 
            END) as \`Delayed\`
        FROM fms_dibiaa_tasks t
        JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id
        WHERE t.plan_date BETWEEN ? AND ?
        GROUP BY s.step_id, s.step_name
        ORDER BY s.step_id ASC`;
    
    try {
        const [rows] = await db.query(sql, [start, end]);
        res.json(rows);
    } catch (error) {
        console.error("SQL Error in FMS Report:", error);
        res.status(500).json({ error: "Internal Server Error", details: error.message });
    }
});

// --- UPLOADS ---
app.post('/tasks/upload', upload.single('file'), async (req, res) => { if (!req.file) return res.status(400).json({ message: "No file uploaded" }); try { const results = []; fs.createReadStream(req.file.path).pipe(csv()).on('data', (data) => results.push(data)).on('end', async () => { const [users] = await db.query("SELECT email, name FROM users"); const bulkTasks = []; const assignedBy = req.body.assigned_by || 'Admin Upload'; for (const row of results) { const empName = users.find(u => u.email === row.employee_email)?.name || row.employee_email; const tDate = parseToMySQLDate(row.target_date); if(tDate) { bulkTasks.push(['T-'+Math.floor(Math.random()*90000), empName, row.employee_email, row.approver_email, row.description, tDate, row.priority || 'Medium', row.approval_needed || 'No', assignedBy, row.remarks || '', 'Pending', 'Pending']); } } if (bulkTasks.length > 0) { await db.query("INSERT INTO tasks (task_uid,employee_name,assigned_to_email,approver_email,description,target_date,priority,approval_needed,assigned_by,remarks,status,previous_status) VALUES ?", [bulkTasks]); } fs.unlinkSync(req.file.path); res.json({ message: `Delegated ${bulkTasks.length} tasks.` }); }); } catch (e) { res.status(500).json({ error: e.message }); } });
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
    const {start,end}=req.query; 
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

app.post('/fms/sync-dibiaa', async (req, res) => {
    try {
        const SHEET_ID = '1C3qHR_jbjHgOQCM7MwRB4AZXtuY2W9jLpqIAEbYFWkQ';
        const auth = getAuth();
        if(!auth) return res.status(500).json({error: "Google Auth Failed"});

        const sheets = google.sheets({ version: 'v4', auth });
        const response = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `fmstask!A2:O` });
        const rows = response.data.values;
        if(!rows || rows.length === 0) return res.json({message: "No data found"});

        const rawValues = [];
        for(let i=0; i<rows.length; i++) {
            const r = rows[i];
            const rowIndex = 2 + i;
            rawValues.push([
                rowIndex, parseToMySQLDateTime(r[0]), r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], parseToMySQLDate(r[13]), r[14]
            ]);
        }

        if(rawValues.length > 0) {
            const rawSql = `INSERT INTO fms_dibiaa_raw (sheet_row_index, timestamp, otd_type, job_number, order_by, company_name, box_type, box_style, box_color, printing_type, printing_color, specification, city, quantity, lead_time, repeat_new) 
                            VALUES ? 
                            ON DUPLICATE KEY UPDATE 
                            otd_type=VALUES(otd_type), box_type=VALUES(box_type), printing_type=VALUES(printing_type), 
                            quantity=VALUES(quantity), company_name=VALUES(company_name), box_style=VALUES(box_style),
                            box_color=VALUES(box_color), printing_color=VALUES(printing_color), specification=VALUES(specification),
                            city=VALUES(city), lead_time=VALUES(lead_time), repeat_new=VALUES(repeat_new)`;
            await db.query(rawSql, [rawValues]);
        }

        const [jobs] = await db.query("SELECT job_id, sheet_row_index FROM fms_dibiaa_raw");
        const jobMap = {}; 
        jobs.forEach(j => jobMap[j.sheet_row_index] = j.job_id);
        
        const [allTasks] = await db.query("SELECT job_id, step_id, actual_date FROM fms_dibiaa_tasks");
        const taskMap = {}; 
        allTasks.forEach(t => taskMap[`${t.job_id}_${t.step_id}`] = t.actual_date ? dayjs.tz(t.actual_date, "Asia/Kolkata") : null);

        const taskValues = [];
        for (let i = 0; i < rows.length; i++) {
            const r = rows[i]; 
            const rowIndex = 2 + i; 
            const jobId = jobMap[rowIndex];
            if(!jobId) continue;
            
            const getAct = (s) => taskMap[`${jobId}_${s}`];
            const A = parseDate(r[0]); const B = r[1]; const F = r[5]; const G = r[6]; const I = r[8]; const K = r[10]; const N = parseDate(r[13]);
            
            const hasInner = (K || '').toLowerCase().includes('inner'); 
            const isOffsetFoil = (I === 'Offset Print' || I === 'Foil Print' || I === 'No'); 
            const isScreenPrint = (I === 'Screen print');
            
            let plans = {}; 
            if (I !== 'No' && A) plans[4] = addWorkdays(A, 3);
            const step4Act = getAct(4);
            if ((B==='OTD' || B==='Jewellery (OTD)')) { if (step4Act) plans[1] = addWorkdays(step4Act, 6); }
            
            const step1Act = getAct(1); 
            if ((B === 'OTD' || B === 'Jewellery (OTD)') && step1Act) { plans[2] = addWorkdays(step1Act, 1); } 
            else if (I === 'No' && A ) { plans[2] = addWorkdays(A, 1); } 
            else if (step4Act) { plans[2] = addWorkdays(step4Act, 1); }
            
            if (getAct(2)) plans[3] = addWorkdays(getAct(2), 1); 
            if (!(F==='Paper Bag' || (F||'').endsWith('Tray'))) { if (getAct(2)) plans[5] = addWorkdays(getAct(2), 3); }
            if (I === 'Foil Print' && getAct(3)) plans[6] = addWorkdays(getAct(3), 3);
            if (I !== 'Foil Print' && getAct(3)) plans[7] = addWorkdays(getAct(3), 3); else if (getAct(6)) plans[7] = addWorkdays(getAct(6), 3);
            if (getAct(7)) plans[8] = addWorkdays(getAct(7), 1); 

            if (getAct(8)) { 
                const condition = (G==='Magnetic' || (G||'').startsWith('Sliding Handle') && I === 'Screen print') || (G==='Magnetic' && isOffsetFoil && hasInner) || (G==='Magnetic' && hasInner && I === 'Screen print'); 
                if(condition) plans[9] = addWorkdays(getAct(8), 1); 
            } 

            const isTopBottom = G==='Top-Bottom'; const isSlidingBox = G==='Sliding Box'; const isMagnetic = G==='Magnetic'; 
            const isSlidingHandle = G==='Sliding Handle Box'; const isPaperBag = F==='Paper Bag'; 
            
            let targetDate10 = null; 
            if (isPaperBag && isScreenPrint) targetDate10 = getAct(12);
            else if (isPaperBag && isOffsetFoil) targetDate10 = getAct(8);
            else if (isMagnetic && hasInner) targetDate10 = getAct(11);
            else if ((isMagnetic || isSlidingHandle) && isOffsetFoil) targetDate10 = getAct(8);
            else if ((isMagnetic || isSlidingHandle) && isScreenPrint) targetDate10 = getAct(12);
            else if (isTopBottom && hasInner) targetDate10 = getAct(11);
            else if (isTopBottom || isSlidingBox) targetDate10 = getAct(8);
            if (targetDate10) plans[10] = addWorkdays(targetDate10, 2);

            const base11 = getAct(10) || getAct(9) || getAct(8); 
            if (base11 && hasInner) plans[11] = addWorkdays(base11, 1);
            
            let targetDate12 = null;
            if (isPaperBag && isScreenPrint) targetDate12 = getAct(8);
            else if ((isMagnetic || isSlidingHandle) && I === 'Screen print') targetDate12 = getAct(9);
            else if ((isMagnetic && hasInner) && I === 'Screen print') targetDate12 = getAct(9);
            else if ((isTopBottom && hasInner) && I === 'Screen print') targetDate12 = getAct(10);
            else if ((isTopBottom || isSlidingBox) && I === 'Screen print') targetDate12 = getAct(10);
            if (targetDate12) plans[12] = addWorkdays(targetDate12, 1);

            const isboxtypecon = (F === 'Foam' || F === 'Cards' || F === 'Hooks');
            const base13 = getAct(12) || getAct(11) || getAct(10); 
            if(isboxtypecon) plans[13] = addWorkdays(getAct(8),1);
            else if (base13) plans[13] = addWorkdays(base13, 1);

            if (getAct(13)) plans[14] = addWorkdays(getAct(13), 1); 
            if (getAct(14)) plans[15] = addWorkdays(getAct(14), 1); 
            if (N) plans[16] = N;

            for (let s = 1; s <= 16; s++) { 
                if (plans[s]) { 
                    const isValid = dayjs.isDayjs(plans[s]) && plans[s].isValid();
                    const sqlDate = isValid ? plans[s].format('YYYY-MM-DD HH:mm:ss') : null; 
                    if(sqlDate) taskValues.push([jobId, s, sqlDate, 'Pending']); 
                } 
            }
        } // End of rows loop

        if(taskValues.length > 0) {
            const taskSql = `INSERT INTO fms_dibiaa_tasks (job_id, step_id, plan_date, status) 
                            VALUES ? 
                            ON DUPLICATE KEY UPDATE 
                            plan_date = IF(status = 'Completed', plan_date, VALUES(plan_date))`;
            await db.query(taskSql, [taskValues]);
        }
        res.json({message: `Sync Complete. Processed ${rows.length} rows.`});
    } catch(e) { 
        console.error("Sync Error:", e); 
        res.status(500).json({error: e.message}); 
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
        (SELECT custom_field_1 FROM fms_dibiaa_tasks WHERE job_id = t.job_id AND (custom_field_1 IS NOT NULL AND custom_field_1 != '') ORDER BY actual_date DESC LIMIT 1) as latest_worker,
        (SELECT custom_field_2 FROM fms_dibiaa_tasks WHERE job_id = t.job_id AND (custom_field_2 IS NOT NULL AND custom_field_2 != '') ORDER BY actual_date DESC LIMIT 1) as latest_qty
    FROM fms_dibiaa_tasks t 
    JOIN fms_dibiaa_raw r ON t.job_id = r.job_id 
    JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id 
    WHERE t.status = 'Pending' AND t.step_id IN (?) 
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
    res.json({message: "Task Completed"});

    } catch (error) {
        console.error("Error completing FMS task:", error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

app.get('/fms/rolling-report', async (req, res) => {
    const { start, end, client, city, step } = req.query;
    let params = [start, end];
    
    // Logic: Join with Step 13 to check the QC + Packing Plan status
    let whereClause = `
        WHERE t.plan_date BETWEEN ? AND ? 
        AND t.status = 'Pending'
        AND EXISTS (
            SELECT 1 FROM fms_dibiaa_tasks 
            WHERE job_id = t.job_id 
            AND step_id = 13 
            AND plan_date IS NOT NULL 
            AND (actual_date IS NULL OR actual_date = '')
        )
    `;

    if (client) { whereClause += " AND r.company_name = ?"; params.push(client); }
    if (city) { whereClause += " AND r.city = ?"; params.push(city); }
    if (step) { whereClause += " AND s.step_name = ?"; params.push(step); }

    const sql = `
        SELECT 
            r.job_number, r.order_by, s.step_name, t.plan_date, 
            r.company_name, r.box_type, r.quantity, r.city,
            DATEDIFF(NOW(), t.plan_date) as delay_val
        FROM fms_dibiaa_tasks t
        JOIN fms_dibiaa_raw r ON t.job_id = r.job_id
        JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id
        ${whereClause}
        ORDER BY t.plan_date ASC`;

    try {
        const [rows] = await db.query(sql, params);
        
        const stats = {
            uniqueClients: new Set(rows.map(r => r.company_name)).size,
            totalJobs: rows.length,
            totalQty: rows.reduce((acc, curr) => acc + (Number(curr.quantity) || 0), 0)
        };

        res.json({ data: rows, stats });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/fms/pc-summary', async (req, res) => {
    try {
        const { start, end, clients, steps, jobNumbers, statuses } = req.body;
        
        let params = [start, end];
        let whereClause = "WHERE t.plan_date BETWEEN ? AND ? AND (t.actual_date IS NULL OR t.actual_date = '')";

        // Multi-select Filter logic
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
                r.job_number, 
                r.order_by, 
                t.plan_date, 
                t.employee_email, -- Used for the Pie Chart instead of Name
                r.company_name, 
                s.step_name, 
                r.box_type, 
                r.box_style, 
                r.quantity, 
                r.city
            FROM fms_dibiaa_tasks t
            JOIN fms_dibiaa_raw r ON t.job_id = r.job_id
            JOIN fms_dibiaa_steps_config s ON t.step_id = s.step_id
            ${whereClause}
            ORDER BY t.plan_date ASC`;

        const [rows] = await db.query(sql, params);
        
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
        console.error("PC Summary Error:", error.message);
        res.status(500).json({ error: error.message });
    }
});

app.get('/mis/checklist-report', async (req, res) => {
    const { start, end } = req.query;
    const today = dayjs().format('YYYY-MM-DD');

    const sql = `
        SELECT 
            employee_name, 
            employee_email,
            COUNT(id) as total_task,
            SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) as total_pending,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as total_completed,
            SUM(CASE 
                WHEN (status = 'Pending' AND target_date < ?) OR (status = 'Completed' AND DATE(completed_at) > target_date) 
                THEN 1 ELSE 0 
            END) as total_delayed
        FROM checklist_tasks
        WHERE target_date BETWEEN ? AND ?
        GROUP BY employee_email, employee_name
        HAVING total_task > 0`;

    try {
        const [rows] = await db.query(sql, [today, start, end]);
        res.json(rows);
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
app.post('/fms/dibiaa-config', async (req, res) => { const { step_id, doer_emails, visible_columns } = req.body; await db.query("UPDATE fms_dibiaa_steps_config SET doer_emails=?, visible_columns=? WHERE step_id=?", [doer_emails, visible_columns, step_id]); res.json({message: "Saved"}); });

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
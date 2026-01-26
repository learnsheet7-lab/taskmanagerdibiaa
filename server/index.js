require('dotenv').config();

const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const dayjs = require('dayjs'); 
const customParseFormat = require('dayjs/plugin/customParseFormat');
dayjs.extend(customParseFormat);
const { google } = require('googleapis'); 
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());
app.use(cors());

// --- DATABASE CONNECTION (ENV VARIABLES) ---
const db = mysql.createPool({
    host: process.env.DB_HOST, // Make sure there is NO default 'localhost' here if on Vercel
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    enableKeepAlive: true, // Add this line (helps with Vercel)
    keepAliveInitialDelay: 0 // Add this line
});

// --- GOOGLE AUTH (ENV OR FILE) ---
const getAuth = () => {
    // Priority 1: Environment Variable (For Hostinger/Production)
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

// ... (Rest of logic remains identical) ...

const parseToMySQLDate = (dateStr) => {
    if (!dateStr) return null;
    const cleanStr = dateStr.toString().trim();
    const formats = ['DD/MM/YYYY HH:mm:ss', 'DD/MM/YYYY HH:mm', 'DD/MM/YYYY', 'D/M/YYYY', 'YYYY-MM-DD', 'MM/DD/YYYY'];
    const d = dayjs(cleanStr, formats, true);
    if (d.isValid()) return d.format('YYYY-MM-DD');
    const stdDate = new Date(cleanStr);
    if (!isNaN(stdDate)) return dayjs(stdDate).format('YYYY-MM-DD');
    return null;
};

const parseToMySQLDateTime = (dateStr) => {
    if (!dateStr) return null;
    const cleanStr = dateStr.toString().trim();
    const formats = ['DD/MM/YYYY HH:mm:ss', 'DD/MM/YYYY HH:mm', 'DD-MM-YYYY HH:mm:ss', 'DD/MM/YYYY', 'YYYY-MM-DD HH:mm:ss', 'YYYY-MM-DD'];
    const d = dayjs(cleanStr, formats, true);
    if (d.isValid()) return d.format('YYYY-MM-DD HH:mm:ss');
    const stdDate = new Date(cleanStr);
    if (!isNaN(stdDate)) return dayjs(stdDate).format('YYYY-MM-DD HH:mm:ss');
    return null;
};

// --- AUTH & CRUD ---
app.post('/login', async (req, res) => { try { const [r] = await db.query("SELECT * FROM users WHERE (email = ? OR mobile = ?) AND password = ?", [req.body.identifier, req.body.identifier, req.body.password]); res.json(r.length ? r[0] : { message: "User not found" }); } catch (e) { res.status(500).json(e); } });
app.get('/users', async (req, res) => { const [r] = await db.query("SELECT * FROM users"); res.json(r); });
app.post('/users', async (req, res) => { await db.query("INSERT INTO users (name, role, department, email, mobile, password) VALUES (?,?,?,?,?,?)", [req.body.name, req.body.role, req.body.department, req.body.email, req.body.mobile, req.body.password]); res.json({message:"Created"}); });
app.put('/users/update', async (req, res) => { await db.query("UPDATE users SET name=?, email=?, role=?, department=?, mobile=? WHERE id=?", [req.body.name, req.body.email, req.body.role, req.body.department, req.body.mobile, req.body.id]); res.json({message:"Updated"}); });
app.put('/users/password', async (req, res) => { await db.query("UPDATE users SET password=? WHERE id=?", [req.body.newPassword, req.body.id]); res.json({message:"Updated"}); });
app.get('/holidays', async (req, res) => { const [r] = await db.query("SELECT * FROM holidays ORDER BY holiday_date"); res.json(r); });
app.post('/holidays', async (req, res) => { await db.query("INSERT INTO holidays (holiday_date, name) VALUES (?,?)", [req.body.date, req.body.name]); res.json({message:"Added"}); });

// --- TASKS ---
app.post('/checklist', async (req, res) => { const { description, employee_email, employee_name, frequency, start_date } = req.body; const [h] = await db.query("SELECT holiday_date FROM holidays"); const holidays = new Set(h.map(x=>x.holiday_date)); const tasks=[]; let c=dayjs(start_date); const end=dayjs().endOf('year'); while(c.isBefore(end)||c.isSame(end,'day')){const d=c.format('YYYY-MM-DD'); if((c.day()!==0||d===start_date)&&!holidays.has(d)) tasks.push(['CHK-'+Math.floor(Math.random()*1000),description,employee_email,employee_name,frequency,d,'Pending']); if(frequency==='Daily')c=c.add(1,'day');else if(frequency==='Weekly')c=c.add(1,'week');else if(frequency==='Monthly')c=c.add(1,'month');else if(frequency==='Quarterly')c=c.add(3,'month');else c=c.add(1,'year');} if(tasks.length>0){await db.query("INSERT INTO checklist_tasks (uid,description,employee_email,employee_name,frequency,target_date,status) VALUES ?", [tasks]); res.json({message:`Generated ${tasks.length}`});} else res.json({message:"No dates"}); });
app.get('/checklist/:email/:role', async (req, res) => { const today=dayjs().format('YYYY-MM-DD'); const q=req.params.role==='Admin'?"SELECT * FROM checklist_tasks WHERE (target_date <= ? AND status='Pending') OR target_date = ? ORDER BY target_date ASC":"SELECT * FROM checklist_tasks WHERE employee_email=? AND ((target_date<=? AND status='Pending') OR target_date=?) ORDER BY target_date ASC"; const [r]=await db.query(q,req.params.role==='Admin'?[today,today]:[req.params.email,today,today]); res.json(r); });
app.put('/checklist/complete', async (req, res) => { await db.query("UPDATE checklist_tasks SET status='Completed', completed_at=NOW() WHERE id=?", [req.body.id]); res.json({message:"Done"}); });
app.post('/tasks', async (req, res) => { await db.query("INSERT INTO tasks (task_uid,employee_name,assigned_to_email,approver_email,description,target_date,priority,approval_needed,assigned_by,remarks,status,previous_status) VALUES (?,?,?,?,?,?,?,?,?,?,'Pending','Pending')",['T-'+Math.floor(Math.random()*9000),req.body.employee_name,req.body.email,req.body.approver_email,req.body.description,req.body.target_date,req.body.priority,req.body.approval_needed,req.body.assigned_by||'System',req.body.remarks||'']); res.json({message:"Delegated"}); });
app.get('/tasks/:email/:role', async (req, res) => { const q=req.params.role==='Admin'?"SELECT * FROM tasks ORDER BY created_at DESC":"SELECT * FROM tasks WHERE assigned_to_email=? ORDER BY created_at DESC"; const [r]=await db.query(q,[req.params.email]); res.json(r); });
app.delete('/tasks/:id', async (req, res) => { await db.query("DELETE FROM tasks WHERE id=?", [req.params.id]); res.json({message:"Deleted"}); });
app.put('/tasks/update-status', async (req, res) => { const {id,status,revised_date,remarks,is_rejection}=req.body; if(is_rejection) await db.query("UPDATE tasks SET status = CASE WHEN previous_status IS NULL OR previous_status='' OR previous_status='Waiting Approval' THEN 'Pending' ELSE previous_status END WHERE id=?", [id]); else if(status==='Revision Requested') await db.query("UPDATE tasks SET previous_status=status, status=?, revised_date_request=?, revision_remarks=? WHERE id=?", [status,revised_date,remarks,id]); else if(status==='Revised') await db.query("UPDATE tasks SET status='Revised', target_date=revised_date_request WHERE id=?", [id]); else await db.query("UPDATE tasks SET previous_status=status, status=? WHERE id=?", [status,id]); res.json({message:"Updated"}); });
app.get('/comments/:taskId', async (req, res) => { const [r]=await db.query("SELECT id,task_id,user_name,comment,DATE_FORMAT(created_at,'%d/%m/%Y %H:%i:%s') as formatted_date FROM task_comments WHERE task_id=? ORDER BY created_at DESC",[req.params.taskId]); res.json(r); });
app.post('/comments', async (req, res) => { await db.query("INSERT INTO task_comments (task_id,user_name,comment) VALUES (?,?,?)",[req.body.task_id,req.body.user_name,req.body.comment]); res.json({message:"Added"}); });
app.get('/dashboard/:email/:role', async (req, res) => { const {email,role}=req.params; const base=role==='Admin'?"":`WHERE assigned_to_email='${email}'`; const and=role==='Admin'?"WHERE":"AND"; const [r1]=await db.query(`SELECT COUNT(*) c FROM tasks ${base} ${and} status IN ('Pending','Revision Requested','Waiting Approval')`); const [r2]=await db.query(`SELECT COUNT(*) c FROM tasks ${base} ${and} status='Revised'`); const [r3]=await db.query(`SELECT COUNT(*) c FROM tasks ${base} ${and} status='Completed'`); const [r4]=await db.query(`SELECT * FROM tasks ${base} ${and} target_date='${dayjs().format('YYYY-MM-DD')}' AND status!='Completed'`); res.json({pending:r1[0].c, revised:r2[0].c, completed:r3[0].c, todayTasks:r4}); });
app.get('/approvals/:email', async (req, res) => { const [r]=await db.query("SELECT * FROM tasks WHERE LOWER(approver_email)=LOWER(?) AND status IN ('Waiting Approval','Revision Requested')",[req.params.email]); res.json(r); });

// --- MIS ---
app.get('/mis/report', async (req, res) => { const {start,end}=req.query; const sql=`SELECT u.name as employee_name,u.email as employee_email,(SELECT planned_count FROM employee_plans WHERE employee_email=u.email ORDER BY plan_date DESC LIMIT 1) as planned,COUNT(t.id) as total_task,SUM(CASE WHEN t.status IN ('Pending','Revision Requested','Waiting Approval') THEN 1 ELSE 0 END) as total_pending,SUM(CASE WHEN t.status='Revised' THEN 1 ELSE 0 END) as total_revised,SUM(CASE WHEN t.status='Completed' THEN 1 ELSE 0 END) as total_completed FROM users u LEFT JOIN tasks t ON u.email=t.assigned_to_email AND t.target_date BETWEEN ? AND ? WHERE u.role!='Admin' GROUP BY u.email,u.name HAVING total_task>0`; const [rows]=await db.query(sql,[start,end]); res.json(rows); });
app.get('/mis/tasks', async (req, res) => { const [rows]=await db.query("SELECT description,target_date,status,completed_at FROM tasks WHERE assigned_to_email=? AND target_date BETWEEN ? AND ? ORDER BY target_date",[req.query.email,req.query.start,req.query.end]); res.json(rows); });
app.post('/mis/plan', async (req, res) => { const {email,date,count}=req.body; const [ex]=await db.query("SELECT id FROM employee_plans WHERE employee_email=? AND plan_date=?",[email,date]); if(ex.length>0) await db.query("UPDATE employee_plans SET planned_count=? WHERE id=?",[count,ex[0].id]); else await db.query("INSERT INTO employee_plans (employee_email,plan_date,planned_count) VALUES (?,?,?)",[email,date,count]); res.json({message:"Saved"}); });

// ================= FMS =================
app.post('/fms/fetch-headers', async (req, res) => { try { const { spreadsheet_id, tab_name, header_row } = req.body; const safeRow = (header_row && parseInt(header_row) > 0) ? header_row : 1; const auth = getAuth(); const sheets = google.sheets({ version: 'v4', auth }); const response = await sheets.spreadsheets.values.get({ spreadsheetId: spreadsheet_id, range: `${tab_name}!A${safeRow}:ZZ${safeRow}` }); const headers = response.data.values ? response.data.values[0] : []; res.json(headers.map((h, i) => ({ index: i, name: h }))); } catch(e) { res.status(500).json({ error: e.message }); } });
app.post('/fms/link', async (req, res) => { try { const { sheet_name, spreadsheet_id, tab_name, header_row, list_columns, header_names, id } = req.body; const colsStr = list_columns ? list_columns.join(',') : ''; const headersJson = JSON.stringify(header_names || []); if(id) { await db.query("UPDATE fms_registry SET sheet_name=?, spreadsheet_id=?, tab_name=?, header_row_index=?, list_view_columns=?, header_names=? WHERE id=?", [sheet_name, spreadsheet_id, tab_name, header_row, colsStr, headersJson, id]); res.json({insertId: id}); } else { const [r] = await db.query("INSERT INTO fms_registry (sheet_name, spreadsheet_id, tab_name, header_row_index, list_view_columns, header_names) VALUES (?, ?, ?, ?, ?, ?)", [sheet_name, spreadsheet_id, tab_name, header_row, colsStr, headersJson]); res.json({insertId: r.insertId}); } } catch(e) { res.status(500).json(e); } });
app.post('/fms/config', async (req, res) => { try { const { fms_id, mappings } = req.body; await db.query("DELETE FROM fms_step_config WHERE fms_id = ?", [fms_id]); for (const m of mappings) { const [r] = await db.query("INSERT INTO fms_step_config (fms_id, step_name, plan_col_index, actual_col_index, doer_emails) VALUES (?,?,?,?,?)", [fms_id, m.step_name, m.plan_col, m.actual_col, m.doers.join(',')]); const configId = r.insertId; if (m.inputs && m.inputs.length > 0) { for (const inp of m.inputs) { await db.query("INSERT INTO fms_step_inputs (config_id, input_col_index, input_label, logic_type, logic_value) VALUES (?,?,?,?,?)", [configId, inp.col, inp.label, inp.logic, inp.val]); } } } res.json({message: "Saved"}); } catch(e) { res.status(500).json(e); } });
app.get('/fms/list', async (req, res) => { const [rows] = await db.query("SELECT * FROM fms_registry"); res.json(rows); });
app.get('/fms/config/:id', async (req, res) => { const [configs] = await db.query("SELECT * FROM fms_step_config WHERE fms_id = ?", [req.params.id]); for (let c of configs) { const [inputs] = await db.query("SELECT * FROM fms_step_inputs WHERE config_id = ?", [c.id]); c.inputs = inputs; } res.json(configs); });
app.delete('/fms/:id', async (req, res) => { await db.query("DELETE FROM fms_registry WHERE id = ?", [req.params.id]); await db.query("DELETE FROM fms_tasks_cache WHERE fms_id = ?", [req.params.id]); res.json({message: "Deleted"}); });

app.post('/fms/sync', async (req, res) => {
    try {
        const { fms_id } = req.body;
        const [fms] = await db.query("SELECT * FROM fms_registry WHERE id = ?", [fms_id]);
        if(fms.length===0) return res.status(404).json({error:"FMS not found"});
        const [steps] = await db.query("SELECT * FROM fms_step_config WHERE fms_id = ?", [fms_id]);
        const auth = getAuth();
        const sheets = google.sheets({ version: 'v4', auth });
        const rowStart = parseInt(fms[0].header_row_index) + 1;
        const range = `${fms[0].tab_name}!A${rowStart}:ZZ1000`;
        const response = await sheets.spreadsheets.values.get({ spreadsheetId: fms[0].spreadsheet_id, range });
        const rows = response.data.values;
        if(!rows) return res.json({message: "Empty Sheet"});
        const listCols = fms[0].list_view_columns ? fms[0].list_view_columns.split(',').map(Number) : [0]; 
        let updatedCount = 0;
        for (let i = 0; i < rows.length; i++) {
            const rowData = rows[i];
            const realRowIndex = rowStart + i; 
            const metaObj = {};
            listCols.forEach(colIdx => { metaObj[colIdx] = rowData[colIdx] || '-'; });
            const metaJson = JSON.stringify(metaObj);
            for (const step of steps) {
                const planRaw = rowData[step.plan_col_index];
                const actRaw = rowData[step.actual_col_index];
                const validPlan = parseToMySQLDateTime(planRaw);
                const validAct = parseToMySQLDateTime(actRaw);
                if (validPlan) {
                    const status = validAct ? 'Completed' : 'Pending';
                    const [exists] = await db.query("SELECT id FROM fms_tasks_cache WHERE fms_id=? AND row_index=? AND step_name=?", [fms_id, realRowIndex, step.step_name]);
                    const desc = rowData[0] || `Row ${realRowIndex}`; 
                    if (exists.length === 0) {
                        await db.query("INSERT INTO fms_tasks_cache (fms_id, row_index, task_description, plan_date, actual_date, step_name, status, doer_emails, meta_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [fms_id, realRowIndex, desc, validPlan, validAct, step.step_name, status, step.doer_emails, metaJson]);
                        updatedCount++;
                    } else {
                        await db.query("UPDATE fms_tasks_cache SET plan_date=?, actual_date=?, status=?, meta_data=?, doer_emails=? WHERE id=?", [validPlan, validAct, status, metaJson, step.doer_emails, exists[0].id]);
                    }
                }
            }
        }
        res.json({message: `Synced ${rows.length} rows. Updated ${updatedCount} entries.`});
    } catch(e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.get('/fms/tasks', async (req, res) => { const { user_email, role } = req.query; const [registries] = await db.query("SELECT id, sheet_name, list_view_columns, header_names FROM fms_registry"); let sql = "SELECT * FROM fms_tasks_cache WHERE status='Pending'"; if (role !== 'Admin') sql += ` AND doer_emails LIKE '%${user_email}%'`; const [tasks] = await db.query(sql); const grouped = {}; registries.forEach(reg => { const fmsTasks = tasks.filter(t => t.fms_id === reg.id); if (fmsTasks.length > 0) { const listCols = reg.list_view_columns ? reg.list_view_columns.split(',').map(Number) : []; let allHeaders = []; try { allHeaders = reg.header_names ? JSON.parse(reg.header_names) : []; } catch(e) {} const viewHeaders = listCols.map(idx => allHeaders[idx] || `Col ${idx}`); grouped[reg.sheet_name] = { fms_id: reg.id, column_indexes: listCols, header_names: viewHeaders, tasks: fmsTasks }; } }); res.json(grouped); });
app.get('/fms/step-inputs/:taskId', async (req, res) => { const [task] = await db.query("SELECT * FROM fms_tasks_cache WHERE id = ?", [req.params.taskId]); if(task.length === 0) return res.json([]); const [config] = await db.query("SELECT * FROM fms_step_config WHERE fms_id=? AND step_name=?", [task[0].fms_id, task[0].step_name]); if(config.length === 0) return res.json([]); const [inputs] = await db.query("SELECT * FROM fms_step_inputs WHERE config_id=?", [config[0].id]); res.json(inputs); });
app.post('/fms/complete', async (req, res) => { try { const { task_id, input_values } = req.body; const [task] = await db.query("SELECT * FROM fms_tasks_cache WHERE id = ?", [task_id]); if(task.length === 0) return res.status(404).json({error: "Task not found"}); const t = task[0]; const [config] = await db.query("SELECT * FROM fms_step_config WHERE fms_id=? AND step_name=?", [t.fms_id, t.step_name]); const [fms] = await db.query("SELECT * FROM fms_registry WHERE id=?", [t.fms_id]); const c = config[0]; const s = fms[0]; const auth = getAuth(); const sheets = google.sheets({ version: 'v4', auth }); const dateNow = dayjs().format('DD-MMM-YYYY HH:mm'); const getColLetter = (n) => { let l=''; while(n>=0){ l=String.fromCharCode(n%26+65)+l; n=Math.floor(n/26)-1; } return l; }; const dataToUpdate = []; dataToUpdate.push({ range: `${s.tab_name}!${getColLetter(c.actual_col_index)}${t.row_index}`, values: [[dateNow]] }); if (input_values && input_values.length > 0) { input_values.forEach(inp => { dataToUpdate.push({ range: `${s.tab_name}!${getColLetter(inp.col)}${t.row_index}`, values: [[inp.val]] }); }); } await sheets.spreadsheets.values.batchUpdate({ spreadsheetId: s.spreadsheet_id, resource: { valueInputOption: 'USER_ENTERED', data: dataToUpdate } }); await db.query("UPDATE fms_tasks_cache SET status='Completed', actual_date=NOW() WHERE id=?", [task_id]); res.json({message: "Task Completed"}); } catch(e) { res.status(500).json({ error: e.message }); } });

// ================= ADMIN REPORTS =================
app.post('/reports/delay-by-step', async (req, res) => {
    try {
        const { fms_id, start, end } = req.body;
        const sql = `
            SELECT step_name, SUBSTRING_INDEX(doer_emails, ',', 1) as doer, ROUND(AVG(TIMESTAMPDIFF(HOUR, plan_date, actual_date)), 1) as avg_delay_hours
            FROM fms_tasks_cache 
            WHERE fms_id = ? AND status = 'Completed' AND actual_date > plan_date AND plan_date BETWEEN ? AND ?
            GROUP BY step_name, doer_emails
            ORDER BY avg_delay_hours DESC
        `;
        const [rows] = await db.query(sql, [fms_id, start, end]);
        res.json(rows);
    } catch(e) { res.status(500).json(e); }
});

app.post('/reports/performance', async (req, res) => {
    try {
        const { fms_id, start, end } = req.body;
        const sql = `
            SELECT step_name, COUNT(*) as total_tasks, SUM(CASE WHEN status='Completed' AND actual_date > plan_date THEN 1 ELSE 0 END) as total_delay, SUM(CASE WHEN status='Completed' AND actual_date <= plan_date THEN 1 ELSE 0 END) as total_on_time, SUM(CASE WHEN status='Pending' THEN 1 ELSE 0 END) as total_pending
            FROM fms_tasks_cache
            WHERE fms_id = ? AND plan_date BETWEEN ? AND ?
            GROUP BY step_name
        `;
        const [rows] = await db.query(sql, [fms_id, start, end]);
        res.json(rows);
    } catch(e) { res.status(500).json(e); }
});

// =====================================================
// === NEW CODE FOR VERCEL DEPLOYMENT STARTS HERE ===
// =====================================================

// 1. Tell Node to serve the React files from the 'build' folder
app.use(express.static(path.join(__dirname, 'build')));

// 2. Handle React Routing (catch-all)
// This ensures that if someone refreshes the page on /dashboard, it doesn't crash
// Use /.*/ (without quotes) instead of '*'
app.get(/.*/, (req, res) => {
  res.sendFile(path.join(__dirname, 'build', 'index.html'));
});

// =====================================================
// === NEW CODE ENDS HERE ===
// =====================================================

const PORT = process.env.PORT || 8800;

// Only start the server if we are running locally (Not on Vercel)
if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}

// Export the app so Vercel can run it efficiently
module.exports = app;
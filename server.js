import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import helmet from "helmet";
import morgan from "morgan";
import basicAuth from "basic-auth";
import Database from "better-sqlite3";
import dayjs from "dayjs";
import fs from "fs";
import { stringify } from "csv-stringify";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(helmet());
app.use(morgan("tiny"));
app.use(express.json({ limit: "5mb" }));
app.use(express.urlencoded({ extended: true }));

// static
app.use(express.static(path.join(__dirname, "public")));
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

// config
const DISK_PATH = process.env.DISK_PATH || "./data";
const DB_FILENAME = process.env.DB_FILENAME || "logs.db";
fs.mkdirSync(DISK_PATH, { recursive: true });
const DB_PATH = path.join(DISK_PATH, DB_FILENAME);

// DB & schema
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.exec(`
CREATE TABLE IF NOT EXISTS logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  date TEXT NOT NULL,
  driver_name TEXT,
  driver_email TEXT,
  cc_email TEXT,
  truck_num TEXT,
  start_miles REAL,
  end_miles REAL,
  start_time TEXT,
  end_time TEXT,
  rate_mile REAL,
  rate_hour REAL,
  total_miles REAL,
  total_time TEXT,
  total_detention TEXT,
  total_value_hours REAL,
  gross_pay REAL
);
CREATE TABLE IF NOT EXISTS stops (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  log_id INTEGER NOT NULL,
  stop_no INTEGER,
  type TEXT,
  location TEXT,
  arrive TEXT,
  depart TEXT,
  duration TEXT,
  detention TEXT,
  value_hours REAL,
  grain_phase TEXT,
  FOREIGN KEY (log_id) REFERENCES logs(id) ON DELETE CASCADE
);
`);

// auth
function requireAdmin(req, res, next) {
  const creds = basicAuth(req);
  const u = process.env.BASIC_AUTH_USER;
  const p = process.env.BASIC_AUTH_PASS;
  if (!u || !p) return res.status(500).send("Admin auth not configured.");
  if (!creds || creds.name !== u || creds.pass !== p) {
    res.set("WWW-Authenticate", 'Basic realm="Admin"');
    return res.status(401).send("Authentication required.");
  }
  next();
}

// --- API: ingest a day + stops ---
app.post("/api/logs", (req, res) => {
  const d = req.body || {};
  const insLog = db.prepare(`INSERT INTO logs
  (created_at,date,driver_name,driver_email,cc_email,truck_num,start_miles,end_miles,start_time,end_time,rate_mile,rate_hour,total_miles,total_time,total_detention,total_value_hours,gross_pay)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const info = insLog.run(
    dayjs().toISOString(),
    d.date || dayjs().format("YYYY-MM-DD"),
    d.driver_name || "",
    d.driver_email || "",
    d.cc_email || "",
    d.truck_num || "",
    Number(d.start_miles || 0),
    Number(d.end_miles || 0),
    d.start_time || "",
    d.end_time || "",
    Number(d.rate_mile || 0),
    Number(d.rate_hour || 0),
    Number(d.total_miles || 0),
    d.total_time || "",
    d.total_detention || "",
    Number(d.total_value_hours || 0),
    Number(d.gross_pay || 0)
  );
  const logId = info.lastInsertRowid;

  const insStop = db.prepare(`INSERT INTO stops
    (log_id,stop_no,type,location,arrive,depart,duration,detention,value_hours,grain_phase)
    VALUES (?,?,?,?,?,?,?,?,?,?)`);
  const stops = Array.isArray(d.stops) ? d.stops : [];
  const tx = db.transaction((rows) => {
    for (const s of rows) {
      insStop.run(
        logId,
        Number(s.stop_no || 0),
        s.type || "",
        s.location || "",
        s.arrive || "",
        s.depart || "",
        s.duration || "",
        s.detention || "",
        (s.value_hours === "" ? null : Number(s.value_hours || 0)),
        s.grain_phase || ""
      );
    }
  });
  tx(stops);

  res.json({ ok: true, id: logId });
});

// --- Admin Desk ---
app.get("/admin", requireAdmin, (req, res) => {
  const { from, to, driver } = req.query;
  let where = [];
  let params = [];
  if (from) { where.push("date >= ?"); params.push(from); }
  if (to) { where.push("date <= ?"); params.push(to); }
  if (driver) { where.push("driver_name LIKE ?"); params.push(`%${driver}%`); }
  const whereSQL = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const rows = db.prepare(`SELECT * FROM logs ${whereSQL} ORDER BY date DESC, id DESC`).all(...params);

  const stats = {
    days: rows.length,
    miles: rows.reduce((s,r)=> s + (r.total_miles || 0), 0),
    value: rows.reduce((s,r)=> s + (r.total_value_hours || 0), 0),
    pay: rows.reduce((s,r)=> s + (r.gross_pay || 0), 0),
  };

  res.render("admin", {
    rows,
    filters: { from: from || "", to: to || "", driver: driver || "" },
    stats
  });
});

// --- Exports ---
function sendCsv(res, rows, columns) {
  res.setHeader("Content-Disposition", "attachment; filename=export.csv");
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  const stringifier = stringify({ header: true, columns });
  stringifier.pipe(res);
  for (const r of rows) stringifier.write(r);
  stringifier.end();
}

// Daily logs export
app.get("/admin/export/daily_logs.csv", requireAdmin, (req, res) => {
  const { from, to, driver } = req.query;
  let where = [];
  let params = [];
  if (from) { where.push("date >= ?"); params.push(from); }
  if (to) { where.push("date <= ?"); params.push(to); }
  if (driver) { where.push("driver_name LIKE ?"); params.push(`%${driver}%`); }
  const whereSQL = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const rows = db.prepare(`SELECT * FROM logs ${whereSQL} ORDER BY date ASC, id ASC`).all(...params);
  const out = rows.map(r => ({
    Date: r.date,
    "Driver Name": r.driver_name || "",
    "Driver Email": r.driver_email || "",
    "Truck #": r.truck_num || "",
    "Start Miles": r.start_miles || 0,
    "End Miles": r.end_miles || 0,
    "Start Time": r.start_time || "",
    "End Time": r.end_time || "",
    "Rate/Mile": r.rate_mile || 0,
    "Hourly Rate": r.rate_hour || 0,
    "Total Miles": r.total_miles || 0,
    "Total Time": r.total_time || "",
    "Total Detention": r.total_detention || "",
    "Total Value (hrs)": r.total_value_hours || 0,
    "Gross Pay": r.gross_pay || 0
  }));
  sendCsv(res, out, Object.keys(out[0] || {}));
});

// Stops export
app.get("/admin/export/daily_stops.csv", requireAdmin, (req, res) => {
  const { from, to, driver } = req.query;
  let where = [];
  let params = [];
  if (from) { where.push("l.date >= ?"); params.push(from); }
  if (to) { where.push("l.date <= ?"); params.push(to); }
  if (driver) { where.push("l.driver_name LIKE ?"); params.push(`%${driver}%`); }
  const whereSQL = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const rows = db.prepare(`
    SELECT l.date, l.driver_name, l.truck_num, s.stop_no, s.type, s.location, s.arrive, s.depart, s.duration, s.detention, s.value_hours, s.grain_phase
    FROM stops s
    JOIN logs l ON l.id = s.log_id
    ${whereSQL}
    ORDER BY l.date ASC, l.id ASC, s.stop_no ASC
  `).all(...params);
  const out = rows.map(r => ({
    Date: r.date,
    "Driver Name": r.driver_name || "",
    "Truck #": r.truck_num || "",
    "Stop #": r.stop_no || 0,
    "Type": r.type || "",
    "Location": r.location || "",
    "Arrive": r.arrive || "",
    "Depart": r.depart || "",
    "Duration": r.duration || "",
    "Detention": r.detention || "",
    "Value (hrs)": r.value_hours ?? "",
    "Grain Phase": r.grain_phase || ""
  }));
  sendCsv(res, out, Object.keys(out[0] || {}));
});

// health
app.get("/healthz", (req, res) => res.send("ok"));

const port = process.env.PORT || 10000;
app.listen(port, () => console.log(`listening on ${port}`));

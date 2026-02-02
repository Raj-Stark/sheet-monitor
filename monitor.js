import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { dirname } from "path";
import crypto from "crypto";
import axios from "axios";
import XLSX from "xlsx";
import nodemailer from "nodemailer";
import archiver from "archiver";
import "dotenv/config";

/* ================= PATH SETUP ================= */

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/* ================= CONFIG ================= */

const SHEET_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vTCVz8BJbH4U8ITIl9B6SGOs_QZturLdp5pcRJAsgExc8hie9lqFqrrgaOpxeZKbtGvXKUSvjFTwai8/pub?output=xlsx";

const DATA_DIR = path.join(__dirname, "data");
const STATE_FILE = path.join(DATA_DIR, "state.json");
const SNAPSHOT_DIR = path.join(DATA_DIR, "snapshots");
const EXPORT_DIR = path.join(DATA_DIR, "exports");
const LOCK_FILE = path.join(DATA_DIR, "monitor.lock");

const EMAIL_FROM = "rpal778866@gmail.com";
const EMAIL_TO = "collegecosmoswork@gmail.com";

const DEBUG = true;

// Row ID column name (case-insensitive)
const ROW_ID_COLUMN = (process.env.ROW_ID_COLUMN || "id").trim();

// Lock staleness (ms) — if a previous run died, allow takeover after this
const STALE_LOCK_MS = 10 * 60 * 1000; // 10 minutes

/* ================= UTILS ================= */

function ensureDirs() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(SNAPSHOT_DIR))
    fs.mkdirSync(SNAPSHOT_DIR, { recursive: true });
  if (!fs.existsSync(EXPORT_DIR)) fs.mkdirSync(EXPORT_DIR, { recursive: true });
}

function sanitizeFilename(name) {
  return String(name || "").replace(/[/\\?%*:|"<>]/g, "_");
}

function atomicWriteJson(filepath, data) {
  const dir = path.dirname(filepath);
  const base = path.basename(filepath);
  const tmp = path.join(dir, `.${base}.${process.pid}.${Date.now()}.tmp`);
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2));
  fs.renameSync(tmp, filepath);
}

function atomicWriteText(filepath, text) {
  const dir = path.dirname(filepath);
  const base = path.basename(filepath);
  const tmp = path.join(dir, `.${base}.${process.pid}.${Date.now()}.tmp`);
  fs.writeFileSync(tmp, text);
  fs.renameSync(tmp, filepath);
}

const loadState = () =>
  fs.existsSync(STATE_FILE)
    ? JSON.parse(fs.readFileSync(STATE_FILE, "utf8"))
    : { tabHashes: {}, checkedAt: null };

function saveState(state) {
  atomicWriteJson(STATE_FILE, state);
}

/**
 * Snapshot format (new):
 *   { headers: string[], rows: object[] }
 *
 * Backward compatible with old snapshot (array): treat as rows-only.
 */
function loadSnapshot(tabName) {
  const file = path.join(SNAPSHOT_DIR, sanitizeFilename(tabName) + ".json");
  if (!fs.existsSync(file)) return { headers: [], rows: [] };

  const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
  if (Array.isArray(parsed)) return { headers: [], rows: parsed };
  if (parsed && typeof parsed === "object") {
    return {
      headers: Array.isArray(parsed.headers) ? parsed.headers : [],
      rows: Array.isArray(parsed.rows) ? parsed.rows : [],
    };
  }
  return { headers: [], rows: [] };
}

function saveSnapshot(tabName, snapshot) {
  const file = path.join(SNAPSHOT_DIR, sanitizeFilename(tabName) + ".json");
  atomicWriteJson(file, snapshot);
}

function deleteSnapshot(tabName) {
  const file = path.join(SNAPSHOT_DIR, sanitizeFilename(tabName) + ".json");
  if (fs.existsSync(file)) fs.unlinkSync(file);
}

const hash = (data) => crypto.createHash("sha256").update(data).digest("hex");

/* ================= LOCK ================= */

function acquireLock() {
  ensureDirs();

  try {
    // Attempt exclusive create
    const fd = fs.openSync(LOCK_FILE, "wx");
    const payload = {
      pid: process.pid,
      startedAt: new Date().toISOString(),
    };
    fs.writeFileSync(fd, JSON.stringify(payload));
    fs.closeSync(fd);

    if (DEBUG) console.log(`Lock acquired: ${LOCK_FILE}`);
    return true;
  } catch (e) {
    // Lock exists — check staleness
    try {
      const stat = fs.statSync(LOCK_FILE);
      const age = Date.now() - stat.mtimeMs;

      if (age > STALE_LOCK_MS) {
        if (DEBUG)
          console.log(
            `Stale lock detected (age ${Math.round(age / 1000)}s). Removing...`
          );
        fs.unlinkSync(LOCK_FILE);

        // Retry once
        const fd = fs.openSync(LOCK_FILE, "wx");
        const payload = {
          pid: process.pid,
          startedAt: new Date().toISOString(),
          staleRecovered: true,
        };
        fs.writeFileSync(fd, JSON.stringify(payload));
        fs.closeSync(fd);

        if (DEBUG) console.log(`Lock re-acquired after stale recovery.`);
        return true;
      }
    } catch {
      // ignore
    }

    console.error(
      "✗ Another monitor instance is already running (lock exists)."
    );
    return false;
  }
}

function releaseLock() {
  try {
    if (fs.existsSync(LOCK_FILE)) fs.unlinkSync(LOCK_FILE);
    if (DEBUG) console.log(`Lock released: ${LOCK_FILE}`);
  } catch {
    // ignore
  }
}

/* ================= SHEET EXTRACTION ================= */

function normalizeCell(v) {
  // Normalize to string; keep simple but stable.
  // Avoiding aggressive transforms to not “hide” real changes.
  if (v === null || v === undefined) return "";
  return String(v);
}

function isRowAllEmpty(rowObj) {
  const vals = Object.values(rowObj || {});
  return vals.length === 0 || vals.every((v) => normalizeCell(v).trim() === "");
}

/**
 * Extract headers and rows for a single sheet.
 * - Ignores fully empty rows.
 * - Produces stable headers.
 */
function extractSheet(sheet) {
  if (!sheet || !sheet["!ref"]) return { headers: [], rows: [] };

  const matrix = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    blankrows: true,
    defval: "",
  });

  const headers = (matrix[0] || []).map((h) => String(h || "").trim());

  const rows = [];
  for (let r = 1; r < matrix.length; r++) {
    const rowArr = matrix[r] || [];
    const row = {};
    for (let c = 0; c < headers.length; c++) {
      const col = headers[c] || `__EMPTY_${c}`;
      row[col] = normalizeCell(rowArr[c] ?? "");
    }

    // Ignore fully empty rows
    if (!isRowAllEmpty(row)) rows.push(row);
  }

  return { headers, rows };
}

/**
 * Hash an entire sheet (row-by-row) into SHA-256 (memory efficient).
 * We hash the raw matrix including header row. This will detect:
 * - data changes
 * - header changes
 * - row additions/removals
 */
function hashSheet(sheet) {
  if (!sheet || !sheet["!ref"]) return hash("");

  const matrix = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    blankrows: true,
    defval: "",
  });

  const hasher = crypto.createHash("sha256");
  for (const row of matrix) {
    hasher.update(JSON.stringify(row));
  }
  return hasher.digest("hex");
}

/**
 * Export a sheet to CSV and return file path.
 */
function exportSheetToCSV(tabName, sheet) {
  const csv = XLSX.utils.sheet_to_csv(sheet);
  const filename = `${sanitizeFilename(tabName)}_${Date.now()}.csv`;
  const filepath = path.join(EXPORT_DIR, filename);
  atomicWriteText(filepath, csv);
  return filepath;
}

/* ================= HEADER DIFF ================= */

function diffHeaders(prevHeaders, currHeaders) {
  const changes = [];

  const prev = (prevHeaders || [])
    .map((h) => String(h || "").trim())
    .filter(Boolean);
  const curr = (currHeaders || [])
    .map((h) => String(h || "").trim())
    .filter(Boolean);

  const prevSet = new Set(prev);
  const currSet = new Set(curr);

  const added = curr.filter((h) => !prevSet.has(h));
  const removed = prev.filter((h) => !currSet.has(h));

  if (added.length) {
    changes.push({
      row: "HEADER",
      column: "—",
      type: "Header Added",
      before: "",
      after: added.join(", "),
      severity: "STRUCTURAL",
    });
  }

  if (removed.length) {
    changes.push({
      row: "HEADER",
      column: "—",
      type: "Header Removed",
      before: removed.join(", "),
      after: "",
      severity: "STRUCTURAL",
    });
  }

  // Detect order changes (only if same set)
  const sameSet =
    prev.length === curr.length &&
    prev.every((h) => currSet.has(h)) &&
    curr.every((h) => prevSet.has(h));

  if (sameSet) {
    const orderChanged = prev.some((h, i) => curr[i] !== h);
    if (orderChanged) {
      changes.push({
        row: "HEADER",
        column: "—",
        type: "Header Order Changed",
        before: prev.join(" | "),
        after: curr.join(" | "),
        severity: "STRUCTURAL",
      });
    }
  }

  return changes;
}

/* ================= ROW-ID DIFF ENGINE ================= */

/**
 * Get a stable row key.
 * - Uses ROW_ID_COLUMN (case-insensitive) if present
 * - If missing/blank, falls back to a hashed signature of the row contents (excluding empty columns)
 *   This fallback preserves behavior without breaking, but ID column is strongly recommended.
 */
function getRowKey(row) {
  if (!row || typeof row !== "object") return null;

  // Find exact header match ignoring case
  const keys = Object.keys(row);
  const idKey = keys.find(
    (k) => String(k).trim().toLowerCase() === ROW_ID_COLUMN.toLowerCase()
  );

  if (idKey) {
    const raw = normalizeCell(row[idKey]).trim();
    if (raw) return `id:${raw}`;
  }

  // Fallback signature (stable-ish, but will change if any cell changes)
  // This prevents index-based avalanche and avoids breaking if no ID exists.
  const stablePairs = keys
    .filter((k) => k && !k.startsWith("__EMPTY"))
    .map((k) => [k, normalizeCell(row[k]).trim()])
    .filter(([, v]) => v !== "")
    .sort(([a], [b]) => a.localeCompare(b));

  const sig = hash(JSON.stringify(stablePairs));
  return `sig:${sig}`;
}

/**
 * Build map: key -> row
 * Handles duplicates by appending "#2", "#3" etc.
 */
function indexRowsByKey(rows) {
  const map = new Map();
  const counts = new Map();

  for (const row of rows) {
    const baseKey = getRowKey(row);
    if (!baseKey) continue;

    const n = (counts.get(baseKey) || 0) + 1;
    counts.set(baseKey, n);

    const key = n === 1 ? baseKey : `${baseKey}#${n}`;
    map.set(key, row);
  }

  return map;
}

/**
 * Diff rows by Row-ID (key), not by index.
 * Returns changes list with "row" field as the Row-ID key.
 */
function diffRowsById(prevRows, currRows) {
  const changes = [];
  const MAX_CHANGES = 200;

  const prevMap = indexRowsByKey(prevRows);
  const currMap = indexRowsByKey(currRows);

  const allKeys = new Set([...prevMap.keys(), ...currMap.keys()]);

  // Prefer structural changes first when truncating
  const structural = [];
  const dataChanges = [];

  for (const key of allKeys) {
    if (structural.length + dataChanges.length >= MAX_CHANGES) break;

    const oldRow = prevMap.get(key);
    const newRow = currMap.get(key);

    // Added
    if (!oldRow && newRow) {
      structural.push({
        row: key,
        column: "ROW",
        type: "Row Added",
        before: "",
        after: "Row created",
        severity: "STRUCTURAL",
      });
      continue;
    }

    // Deleted
    if (oldRow && !newRow) {
      structural.push({
        row: key,
        column: "ROW",
        type: "Row Deleted",
        before: "Row existed",
        after: "",
        severity: "STRUCTURAL",
      });
      continue;
    }

    if (!oldRow || !newRow) continue;

    const cols = new Set([...Object.keys(oldRow), ...Object.keys(newRow)]);
    for (const col of cols) {
      if (!col || String(col).startsWith("__EMPTY")) continue;

      const before = normalizeCell(oldRow[col]).trim();
      const after = normalizeCell(newRow[col]).trim();

      if (before !== after) {
        let type = "Updated";
        if (!before && after) type = "Added";
        if (before && !after) type = "Cleared";

        dataChanges.push({
          row: key,
          column: col,
          type,
          before,
          after,
          severity: "DATA",
        });

        if (structural.length + dataChanges.length >= MAX_CHANGES) break;
      }
    }
  }

  const combined = [...structural, ...dataChanges];

  // Truncation message (after prioritizing structural)
  if (combined.length >= MAX_CHANGES) {
    combined.push({
      row: "...",
      column: "—",
      type: "Truncated",
      before: "",
      after: `Showing first ${MAX_CHANGES} changes only`,
      severity: "INFO",
    });
  }

  if (DEBUG) {
    console.log(
      `    DEBUG: prevRows=${prevRows.length}, currRows=${currRows.length}, keys(prev)=${prevMap.size}, keys(curr)=${currMap.size}, changes=${combined.length}`
    );
  }

  return combined;
}

/* ================= ZIP HELPERS ================= */

async function zipFiles(filePaths, zipPath) {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver("zip", { zlib: { level: 9 } });

    output.on("close", () => resolve());
    output.on("error", (err) => reject(err));

    archive.on("error", (err) => reject(err));

    archive.pipe(output);

    for (const fp of filePaths) {
      archive.file(fp, { name: path.basename(fp) });
    }

    archive.finalize();
  });
}

/* ================= EMAIL ================= */

const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: EMAIL_FROM,
    pass: process.env.GMAIL_APP_PASSWORD,
  },
});

async function sendEmail({ allChanges, addedTabs, deletedTabs, attachments }) {
  // Group changes by tab then row-key
  const grouped = {};
  for (const [tab, changes] of Object.entries(allChanges)) {
    grouped[tab] = {};
    for (const c of changes) {
      const k = String(c.row);
      grouped[tab][k] ||= [];
      grouped[tab][k].push(c);
    }
  }

  const allChangesList = Object.values(allChanges).flat();
  const totalData = allChangesList.filter((c) => c.severity === "DATA").length;
  const totalStructural = allChangesList.filter(
    (c) => c.severity === "STRUCTURAL"
  ).length;

  const summary = `
    <ul>
      <li><strong>Tabs Changed:</strong> ${Object.keys(allChanges).length}</li>
      <li><strong>Tabs Added:</strong> ${addedTabs.length}</li>
      <li><strong>Tabs Deleted:</strong> ${deletedTabs.length}</li>
      <li><strong>Structural Changes:</strong> ${totalStructural}</li>
      <li><strong>Data Changes:</strong> ${totalData}</li>
    </ul>
  `;

  const sections = [];

  if (addedTabs.length) {
    sections.push(
      `<h3 style="color:#2e7d32">[NEW] Tabs Added</h3>
       <ul>${addedTabs.map((t) => `<li>${t}</li>`).join("")}</ul>`
    );
  }

  if (deletedTabs.length) {
    sections.push(
      `<h3 style="color:#c53030">[REMOVED] Tabs Deleted</h3>
       <ul>${deletedTabs.map((t) => `<li>${t}</li>`).join("")}</ul>`
    );
  }

  for (const [tab, rows] of Object.entries(grouped)) {
    let tabHtml = `<h3 style="color:#1565c0">Tab: ${tab}</h3>`;
    tabHtml += `<p style="margin:8px 0"><strong>📎 Changed tabs are attached as ZIP (CSV per tab)</strong></p>`;

    for (const [rowKey, rowChanges] of Object.entries(rows)) {
      const isStructural = rowChanges.some((c) => c.severity === "STRUCTURAL");

      const rowsHtml = rowChanges
        .map(
          (c) => `
          <tr>
            <td>${c.column}</td>
            <td><strong>${c.type}</strong></td>
            <td style="color:#c53030">${c.before || "<i>blank</i>"}</td>
            <td style="color:#2e7d32">${c.after || "<i>blank</i>"}</td>
          </tr>
        `
        )
        .join("");

      tabHtml += `
        <h4 style="margin-top:12px">
          Row ${rowKey} ${
        isStructural ? `<span style="color:#c53030">(STRUCTURAL)</span>` : ""
      }
        </h4>
        <table border="1" cellpadding="5" cellspacing="0" width="100%" style="border-collapse:collapse">
          <thead>
            <tr style="background:#f0f4f8">
              <th>Column</th>
              <th>Change</th>
              <th>Before</th>
              <th>After</th>
            </tr>
          </thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      `;
    }

    sections.push(tabHtml);
  }

  await transporter.sendMail({
    from: EMAIL_FROM,
    to: EMAIL_TO,
    subject: `[Sheet Alert] ${Object.keys(allChanges).length} tab(s) updated`,
    html: `
      <div style="font-family:Arial,sans-serif; max-width:900px; margin:0 auto">
        <h2>Google Sheet Update Summary</h2>
        ${summary}
        ${sections.join("<hr/>")}
        <p style="font-size:11px;color:#999;margin-top:24px">Auto-generated at ${new Date().toISOString()}</p>
      </div>
    `,
    attachments,
  });
}

/* ================= MAIN ================= */

(async () => {
  let locked = false;

  try {
    ensureDirs();
    locked = acquireLock();
    if (!locked) return;

    const state = loadState();
    const isFirstRun = Object.keys(state.tabHashes || {}).length === 0;

    // Download XLSX
    console.log("Downloading sheet...");
    const res = await axios.get(SHEET_URL, {
      responseType: "arraybuffer",
      timeout: 60000,
    });

    const workbook = XLSX.read(res.data, { type: "buffer" });
    console.log(`Sheets found: ${workbook.SheetNames.join(", ")}`);

    // Step 1: Hash every tab (fast)
    const currentHashes = {};
    for (const name of workbook.SheetNames) {
      currentHashes[name] = hashSheet(workbook.Sheets[name]);
    }

    // First run: save baseline hashes + snapshots, no email
    if (isFirstRun) {
      for (const name of workbook.SheetNames) {
        const extracted = extractSheet(workbook.Sheets[name]);
        saveSnapshot(name, extracted);
      }
      saveState({
        tabHashes: currentHashes,
        checkedAt: new Date().toISOString(),
      });
      console.log(
        `First run - baseline saved. (${workbook.SheetNames.length} tabs)`
      );
      return;
    }

    // Step 2: Find which tabs changed by comparing hashes
    const prevTabNames = Object.keys(state.tabHashes || {});
    const currTabNames = workbook.SheetNames;

    const addedTabs = currTabNames.filter((t) => !prevTabNames.includes(t));
    const deletedTabs = prevTabNames.filter((t) => !currTabNames.includes(t));
    const changedTabs = currTabNames.filter(
      (t) => state.tabHashes[t] && state.tabHashes[t] !== currentHashes[t]
    );

    console.log(
      `Tabs: ${currTabNames.length} | Changed: ${changedTabs.length} | Added: ${addedTabs.length} | Deleted: ${deletedTabs.length}`
    );

    // Step 3: Diff only changed tabs (row-ID diff + header diff)
    const allChanges = {};
    const exportedCsvPaths = [];

    // IMPORTANT: We DO NOT write new snapshots/state yet.
    // We stage them, and commit ONLY after successful email send.
    const stagedSnapshots = new Map(); // tab -> {headers, rows}
    const tabsNeedingCsv = new Set();

    for (const tab of changedTabs) {
      const prevSnap = loadSnapshot(tab);
      const currSnap = extractSheet(workbook.Sheets[tab]);

      const headerChanges = diffHeaders(prevSnap.headers, currSnap.headers);
      const rowChanges = diffRowsById(prevSnap.rows, currSnap.rows);

      const changes = [...headerChanges, ...rowChanges];

      console.log(`  "${tab}": ${changes.length} change(s) detected`);

      if (changes.length > 0) {
        allChanges[tab] = changes;
        tabsNeedingCsv.add(tab);
      }

      stagedSnapshots.set(tab, currSnap);
    }

    // Stage snapshots for newly added tabs
    for (const tab of addedTabs) {
      const currSnap = extractSheet(workbook.Sheets[tab]);
      stagedSnapshots.set(tab, currSnap);
    }

    // Prepare attachments (ZIP of CSVs) only if we will email
    const willEmail =
      Object.keys(allChanges).length || addedTabs.length || deletedTabs.length;

    let attachments = [];

    if (willEmail) {
      // Export CSVs only for tabs that actually have changes recorded
      for (const tab of tabsNeedingCsv) {
        const csvPath = exportSheetToCSV(tab, workbook.Sheets[tab]);
        exportedCsvPaths.push(csvPath);
        if (DEBUG)
          console.log(`    ✓ Exported "${tab}" to ${path.basename(csvPath)}`);
      }

      // Zip them into one attachment (reduces Gmail attachment issues + clean email)
      if (exportedCsvPaths.length) {
        const zipName = `changed_tabs_${Date.now()}.zip`;
        const zipPath = path.join(EXPORT_DIR, zipName);
        await zipFiles(exportedCsvPaths, zipPath);

        attachments.push({
          filename: zipName,
          path: zipPath,
        });

        if (DEBUG)
          console.log(
            `    ✓ Zipped ${exportedCsvPaths.length} CSV(s) into ${zipName}`
          );
      }

      // Step 4: Send email
      await sendEmail({ allChanges, addedTabs, deletedTabs, attachments });
      console.log(
        "✓ Changes detected → email sent (ZIP attached where applicable)"
      );

      // Step 5: COMMIT snapshots + state ONLY AFTER email success
      for (const [tab, snap] of stagedSnapshots.entries()) {
        saveSnapshot(tab, snap);
      }

      for (const tab of deletedTabs) {
        deleteSnapshot(tab);
      }

      saveState({
        tabHashes: currentHashes,
        checkedAt: new Date().toISOString(),
      });

      console.log("✓ State + snapshots committed after email success");
    } else {
      console.log(
        "✓ No detectable changes (hash differences may be formatting/metadata only)"
      );
      // No email -> still safe to update snapshots/state (since nothing to alert)
      // But to keep behavior strict: only commit if hashes show no change. Here, willEmail is false.
      saveState({
        tabHashes: currentHashes,
        checkedAt: new Date().toISOString(),
      });
    }
  } catch (err) {
    console.error("✗ Monitor error:", err?.message || err);
    if (DEBUG && err?.stack) console.error(err.stack);

    // IMPORTANT: On error (including email failures), we do NOT commit snapshots/state.
    // Next run will detect the same changes again and re-alert.
  } finally {
    if (locked) releaseLock();
  }
})();

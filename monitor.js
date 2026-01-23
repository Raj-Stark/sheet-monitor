import fs from "fs";
import crypto from "crypto";
import axios from "axios";
import XLSX from "xlsx";
import nodemailer from "nodemailer";
import "dotenv/config";

/* ================= CONFIG ================= */

const SHEET_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vSt2cvTqROnPbXURx-AQbn9wIXLgLUicI3tnd6akeHvwsv3XtXH520b45ev6FjRP789e_q8t4YpkEYJ/pub?output=xlsx";

const STATE_FILE = "./state.json";

const EMAIL_FROM = "rpal778866@gmail.com";
const EMAIL_TO = "sunilcollegecosmos@gmail.com";

/* ================= STATE ================= */

const loadState = () =>
  fs.existsSync(STATE_FILE)
    ? JSON.parse(fs.readFileSync(STATE_FILE, "utf8"))
    : { tabRows: {} };

const saveState = (state) =>
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));

/* ================= HELPERS ================= */

const hash = (data) => crypto.createHash("sha256").update(data).digest("hex");

/**
 * Extract sheet data while preserving blank rows
 */
function extractTabs(workbook) {
  const tabs = {};

  for (const name of workbook.SheetNames) {
    const sheet = workbook.Sheets[name];
    if (!sheet || !sheet["!ref"]) {
      tabs[name] = [];
      continue;
    }

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
        row[col] = rowArr[c] ?? "";
      }

      rows.push(row);
    }

    tabs[name] = rows;
  }

  return tabs;
}

/* ================= DIFF ENGINE ================= */

function diffTabs(prevTabs, currTabs) {
  const changes = [];
  const prevTabNames = Object.keys(prevTabs);
  const currTabNames = Object.keys(currTabs);

  const addedTabs = currTabNames.filter((t) => !prevTabNames.includes(t));
  const deletedTabs = prevTabNames.filter((t) => !currTabNames.includes(t));

  for (const tab of currTabNames) {
    const oldRows = prevTabs[tab] || [];
    const newRows = currTabs[tab] || [];
    const maxRows = Math.max(oldRows.length, newRows.length);

    for (let i = 0; i < maxRows; i++) {
      const oldRow = oldRows[i];
      const newRow = newRows[i];

      // Row added
      if (!oldRow && newRow && Object.values(newRow).some((v) => v !== "")) {
        changes.push({
          tab,
          row: i + 2,
          column: "ROW",
          type: "Row Added",
          before: "",
          after: "Row created",
          severity: "STRUCTURAL",
        });
        continue;
      }

      // Row deleted
      if (oldRow && !newRow) {
        changes.push({
          tab,
          row: i + 2,
          column: "ROW",
          type: "Row Deleted",
          before: "Row existed",
          after: "",
          severity: "STRUCTURAL",
        });
        continue;
      }

      const cols = new Set([
        ...Object.keys(oldRow || {}),
        ...Object.keys(newRow || {}),
      ]);

      for (const col of cols) {
        if (!col || col.startsWith("__EMPTY")) continue;

        const before = oldRow?.[col] ?? "";
        const after = newRow?.[col] ?? "";

        if (before !== after) {
          let type = "Updated";
          if (!before && after) type = "Added";
          if (before && !after) type = "Cleared";

          changes.push({
            tab,
            row: i + 2,
            column: col,
            type,
            before,
            after,
            severity: "DATA",
          });
        }
      }
    }
  }

  return { changes, addedTabs, deletedTabs };
}

/* ================= EMAIL ================= */

const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: EMAIL_FROM,
    pass: process.env.GMAIL_APP_PASSWORD,
  },
});

async function sendEmail({ changes, addedTabs, deletedTabs }) {
  const grouped = changes.reduce((acc, c) => {
    acc[c.tab] ||= [];
    acc[c.tab].push(c);
    return acc;
  }, {});

  const summary = `
    <ul>
      <li><strong>Tabs Added:</strong> ${addedTabs.length}</li>
      <li><strong>Tabs Deleted:</strong> ${deletedTabs.length}</li>
      <li><strong>Structural Changes:</strong> ${
        changes.filter((c) => c.severity === "STRUCTURAL").length
      }</li>
      <li><strong>Data Changes:</strong> ${
        changes.filter((c) => c.severity === "DATA").length
      }</li>
    </ul>
  `;

  const sections = [];

  if (addedTabs.length) {
    sections.push(
      `<h3>[STRUCTURAL] Tabs Added</h3><ul>${addedTabs
        .map((t) => `<li>${t}</li>`)
        .join("")}</ul>`
    );
  }

  if (deletedTabs.length) {
    sections.push(
      `<h3>[STRUCTURAL] Tabs Deleted</h3><ul>${deletedTabs
        .map((t) => `<li>${t}</li>`)
        .join("")}</ul>`
    );
  }

  for (const [tab, rows] of Object.entries(grouped)) {
    const table = rows
      .map(
        (c) => `
        <tr>
          <td>${c.row}</td>
          <td>${c.column}</td>
          <td><strong>${c.type}</strong></td>
          <td>${c.before || "<i>blank</i>"}</td>
          <td>${c.after || "<i>blank</i>"}</td>
        </tr>
      `
      )
      .join("");

    sections.push(`
      <h3>Tab: ${tab} (${rows.length} changes) [${
      rows.some((r) => r.severity === "STRUCTURAL") ? "STRUCTURAL" : "DATA"
    }]</h3>
      <table border="1" cellpadding="6" cellspacing="0" width="100%">
        <thead>
          <tr>
            <th>Row</th>
            <th>Column</th>
            <th>Change</th>
            <th>Before</th>
            <th>After</th>
          </tr>
        </thead>
        <tbody>${table}</tbody>
      </table>
    `);
  }

  await transporter.sendMail({
    from: EMAIL_FROM,
    to: EMAIL_TO,
    subject: "Google Sheet Updated — Structural & Data Changes",
    html: `
      <div style="font-family:Arial,sans-serif">
        <h2>Google Sheet Update Summary</h2>
        ${summary}
        ${sections.join("<br/>")}
        <p style="font-size:12px;color:#777">Auto-generated</p>
      </div>
    `,
  });
}

/* ================= MAIN ================= */

(async () => {
  try {
    const state = loadState();

    const res = await axios.get(SHEET_URL, {
      responseType: "arraybuffer",
      timeout: 30000,
    });

    const workbook = XLSX.read(res.data, { type: "buffer" });
    const currentTabs = extractTabs(workbook);

    const { changes, addedTabs, deletedTabs } = diffTabs(
      state.tabRows,
      currentTabs
    );

    if (changes.length || addedTabs.length || deletedTabs.length) {
      await sendEmail({ changes, addedTabs, deletedTabs });
      console.log("Changes detected → email sent");
    } else {
      console.log("No change detected");
    }

    saveState({ tabRows: currentTabs, checkedAt: new Date().toISOString() });
  } catch (err) {
    console.error("Monitor error:", err.message);
  }
})();

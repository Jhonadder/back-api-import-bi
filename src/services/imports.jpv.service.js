import fs from "fs";
import { env } from "../config/env.js";
import { getPool, sql } from "../db/pool.js";
import { readExcelFirstSheet } from "./excel.service.js";
import { getTableColumns } from "./sqlMeta.service.js";
import { sha256Hex } from "../utils/cryptoHash.js";

// === parsers (duplicado ok en esta etapa) ===
function parseDecimalAR(v) {
  if (v === null || v === undefined || v === "") return null;

  if (typeof v === "number" && Number.isFinite(v)) return v;

  let s = String(v).replace(/\u00A0/g, " ").trim();

  // negativos contables: (1.234,56)
  let negative = false;
  if (s.startsWith("(") && s.endsWith(")")) {
    negative = true;
    s = s.slice(1, -1).trim();
  }

  // limpia moneda/letras dejando separadores
  s = s.replace(/[^\d,.\- ]/g, "").trim();

  const hasComma = s.includes(",");
  const hasDot = s.includes(".");
  if (hasComma && hasDot) {
    const lastComma = s.lastIndexOf(",");
    const lastDot = s.lastIndexOf(".");
    if (lastComma > lastDot) s = s.replace(/\./g, "").replace(",", ".");
    else s = s.replace(/,/g, "");
  } else if (hasComma) {
    s = s.replace(/\./g, "").replace(",", ".");
  } else {
    s = s.replace(/\s+/g, "");
  }

  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return negative ? -n : n;
}

function parseIntSafe(v) {
  if (v === null || v === undefined || v === "") return null;

  if (typeof v === "number" && Number.isFinite(v)) {
    const ni = Math.trunc(v);
    if (ni < -2147483648 || ni > 2147483647) return null;
    return ni;
  }

  let s = String(v).trim();
  if (!s) return null;

  // soporta "3,00" sin transformarlo en 300
  if (s.includes(",") || s.includes(".")) {
    s = s.replace(/\./g, "").replace(",", ".");
    const nDec = Number(s);
    if (!Number.isFinite(nDec)) return null;
    const ni = Math.trunc(nDec);
    if (ni < -2147483648 || ni > 2147483647) return null;
    return ni;
  }

  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  const ni = Math.trunc(n);
  if (ni < -2147483648 || ni > 2147483647) return null;
  return ni;
}

/**
 * Convierte Excel serial date (días desde 1899-12-30) a Date local "naive"
 */
function excelSerialToDate(serial) {
  // Excel usa 1899-12-30 como base (incluye bug de 1900)
  const ms = (serial - 25569) * 86400 * 1000; // 25569 = 1970-01-01
  const d = new Date(ms);
  if (isNaN(d.getTime())) return null;
  return new Date(d.getFullYear(), d.getMonth(), d.getDate(), d.getHours(), d.getMinutes(), d.getSeconds(), 0);
}

function parseDateSafe(v) {
  if (v === null || v === undefined || v === "") return null;

  // 1) Si vino Date real
  if (v instanceof Date && !isNaN(v.getTime())) {
    return new Date(v.getFullYear(), v.getMonth(), v.getDate(), v.getHours(), v.getMinutes(), v.getSeconds(), 0);
  }

  // 2) Si vino como número (serial excel)
  if (typeof v === "number" && Number.isFinite(v)) {
    // serial típico suele ser > 20000; igual lo intentamos
    return excelSerialToDate(v);
  }

  const s = String(v).replace(/\u00A0/g, " ").trim();
  if (!s) return null;

  // 3) dd/mm/yyyy o dd-mm-yyyy (con hora opcional)
  // ✅ tu caso: "12-01-2025 23:59:02"
  let m = s.match(
    /^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (m) {
    const dd = Number(m[1]);
    const mm = Number(m[2]) - 1;
    const yyyy = Number(m[3]);
    const hh = Number(m[4] || 0);
    const mi = Number(m[5] || 0);
    const ss = Number(m[6] || 0);
    const d = new Date(yyyy, mm, dd, hh, mi, ss, 0);
    return isNaN(d.getTime()) ? null : d;
  }

  // 4) yyyy-mm-dd (con hora opcional)
  m = s.match(
    /^(\d{4})[\/-](\d{1,2})[\/-](\d{1,2})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (m) {
    const yyyy = Number(m[1]);
    const mm = Number(m[2]) - 1;
    const dd = Number(m[3]);
    const hh = Number(m[4] || 0);
    const mi = Number(m[5] || 0);
    const ss = Number(m[6] || 0);
    const d = new Date(yyyy, mm, dd, hh, mi, ss, 0);
    return isNaN(d.getTime()) ? null : d;
  }

  // 5) Último intento: Date nativo (puede aplicar timezone, por eso lo dejamos al final)
  const d = new Date(s);
  if (!isNaN(d.getTime())) {
    return new Date(d.getFullYear(), d.getMonth(), d.getDate(), d.getHours(), d.getMinutes(), d.getSeconds(), 0);
  }

  return null;
}

function toSqlType(col) {
  switch (col.DATA_TYPE) {
    case "datetime2": return sql.DateTime2;
    case "int": return sql.Int;
    case "decimal":
    case "numeric": return sql.Decimal(col.NUMERIC_PRECISION ?? 18, col.NUMERIC_SCALE ?? 2);
    case "nvarchar":
      if (col.CHARACTER_MAXIMUM_LENGTH === -1) return sql.NVarChar(sql.MAX);
      return sql.NVarChar(col.CHARACTER_MAXIMUM_LENGTH ?? 255);
    default:
      return sql.NVarChar(sql.MAX);
  }
}

function convertByType(sqlTypeName, v, { colName, rowNumber } = {}) {
  let out = null;

  switch (sqlTypeName) {
    case "datetime2":
      out = parseDateSafe(v);
      break;
    case "int":
      out = parseIntSafe(v);
      break;
    case "decimal":
    case "numeric":
      out = parseDecimalAR(v);
      break;
    default:
      out = (v == null ? null : String(v));
      break;
  }

  // ✅ Debug: si el Excel traía algo pero lo terminamos guardando NULL, avisamos
  if ((v !== null && v !== undefined && String(v).trim() !== "") && (out === null || out === undefined)) {
    console.warn(`[IMPORT WARN] Valor convertido a NULL -> col="${colName}" row=${rowNumber} raw="${String(v)}" sqlType="${sqlTypeName}"`);
  }

  return out;
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export async function runImportJPV({ filePath, originalName }) {
  const schema = env.import.schema; // importacionxls
  const tableName = "JPV";

  const pool = await getPool();

  // 1) Leer excel
  const { columns: excelCols, rows } = readExcelFirstSheet(filePath);

  // 2) Metadata de tabla
  const dbCols = await getTableColumns(schema, tableName);
  const dbColNames = dbCols.map(c => c.COLUMN_NAME);
  const typeMap = new Map(dbCols.map(c => [c.COLUMN_NAME, c]));

  // 3) Columnas a insertar
  const metaCols = ["__ImportedAt", "__SourceFileName", "__RowNumber", "__RowHash"];
  const insertExcelCols = excelCols.filter(c => dbColNames.includes(c));
  const insertCols = [...metaCols, ...insertExcelCols];

  if (!insertExcelCols.length) {
    try { fs.unlinkSync(filePath); } catch {}
    return {
      ok: true,
      tableName,
      attemptedRows: 0,
      insertedRows: 0,
      skippedDuplicates: 0,
      note: "No matchearon columnas del excel con la tabla JPV."
    };
  }

  const batchSize = env.import.batchSize || 2000;
  const batches = chunk(rows, batchSize);

  const now = new Date();
  let attempted = 0;

  // ✅ COUNT BEFORE (por archivo)
  const beforeRs = await pool.request()
    .input("File", sql.NVarChar(260), originalName)
    .query(`
      SELECT COUNT(*) AS C
      FROM ${schema}.[${tableName}]
      WHERE __SourceFileName = @File
    `);

  const beforeCount = Number(beforeRs.recordset?.[0]?.C ?? 0);

  try {
    for (let b = 0; b < batches.length; b++) {
      const part = new sql.Table(`${schema}.${tableName}`);
      part.create = false;

      for (const colName of insertCols) {
        const colMeta = typeMap.get(colName);
        const t = colMeta ? toSqlType(colMeta) : sql.NVarChar(sql.MAX);
        const nullable = colMeta ? (colMeta.IS_NULLABLE === "YES") : true;
        part.columns.add(colName, t, { nullable });
      }

      const batchRows = batches[b];

      batchRows.forEach((r, idxLocal) => {
        const absoluteRow = b * batchSize + idxLocal; // 0-based

        const payload = insertExcelCols.map(c => String(r?.[c] ?? "").trim()).join("|");
        const rowHash = sha256Hex(payload);

        const values = [];
        values.push(now);
        values.push(originalName);
        values.push(absoluteRow + 1); // __RowNumber
        values.push(rowHash);

        for (const c of insertExcelCols) {
          const meta = typeMap.get(c);
          const v = convertByType(meta?.DATA_TYPE, r?.[c], { colName: c, rowNumber: absoluteRow + 1 });
          values.push(v);
        }

        part.rows.add(...values);
      });

      attempted += batchRows.length;
      await pool.request().bulk(part);
    }

    // ✅ COUNT AFTER
    const afterRs = await pool.request()
      .input("File", sql.NVarChar(260), originalName)
      .query(`
        SELECT COUNT(*) AS C
        FROM ${schema}.[${tableName}]
        WHERE __SourceFileName = @File
      `);

    const afterCount = Number(afterRs.recordset?.[0]?.C ?? 0);

    const insertedRows = Math.max(0, afterCount - beforeCount);
    const skippedDuplicates = Math.max(0, attempted - insertedRows);

    // ✅ ImportRuns REAL
    await pool.request()
      .input("TableName", sql.NVarChar(128), tableName)
      .input("SourceFileName", sql.NVarChar(260), originalName)
      .input("InsertedRows", sql.Int, insertedRows)
      .input("SkippedDuplicates", sql.Int, skippedDuplicates)
      .input("Status", sql.NVarChar(20), "SUCCESS")
      .query(`
        INSERT INTO dbo.ImportRuns (TableName, SourceFileName, InsertedRows, SkippedDuplicates, Status)
        VALUES (@TableName, @SourceFileName, @InsertedRows, @SkippedDuplicates, @Status)
      `);

    return {
      ok: true,
      tableName,
      attemptedRows: attempted,
      insertedRows,
      skippedDuplicates,
      note: "Duplicados se ignoran por RowHash si existe índice UNIQUE con IGNORE_DUP_KEY=ON.",
      insertedRowsApprox: attempted,
    };
  } catch (e) {
    await pool.request()
      .input("TableName", sql.NVarChar(128), tableName)
      .input("SourceFileName", sql.NVarChar(260), originalName)
      .input("InsertedRows", sql.Int, 0)
      .input("SkippedDuplicates", sql.Int, 0)
      .input("Status", sql.NVarChar(20), "FAILED")
      .input("ErrorMessage", sql.NVarChar(sql.MAX), e.message)
      .query(`
        INSERT INTO dbo.ImportRuns (TableName, SourceFileName, InsertedRows, SkippedDuplicates, Status, ErrorMessage)
        VALUES (@TableName, @SourceFileName, @InsertedRows, @SkippedDuplicates, @Status, @ErrorMessage)
      `);

    throw e;
  } finally {
    try { fs.unlinkSync(filePath); } catch {}
  }
}

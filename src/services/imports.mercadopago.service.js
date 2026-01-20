import fs from "fs";
import { env } from "../config/env.js";
import { getPool, sql } from "../db/pool.js";
import { readExcelFirstSheet } from "./excel.service.js";
import { getTableColumns } from "./sqlMeta.service.js";
import { sha256Hex } from "../utils/cryptoHash.js";

function parseDecimalAR(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;

  const s = String(v).trim()
    .replace(/\./g, "")
    .replace(",", ".");

  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function parseIntSafe(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return Math.trunc(v);
  const n = Number(String(v).replace(/[^\d\-]/g, ""));
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function parseDateSafe(v) {
  if (v === null || v === undefined || v === "") return null;

  if (v instanceof Date && !isNaN(v.getTime())) {
    return new Date(
      v.getFullYear(),
      v.getMonth(),
      v.getDate(),
      v.getHours(),
      v.getMinutes(),
      v.getSeconds(),
      0
    );
  }

  const s = String(v).trim();

  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const dd = Number(m[1]);
    const mm = Number(m[2]) - 1;
    const yyyy = Number(m[3]);
    const hh = Number(m[4] || 0);
    const mi = Number(m[5] || 0);
    const ss = Number(m[6] || 0);
    return new Date(yyyy, mm, dd, hh, mi, ss, 0);
  }

  const d = new Date(s);
  if (!isNaN(d.getTime())) {
    return new Date(
      d.getFullYear(),
      d.getMonth(),
      d.getDate(),
      d.getHours(),
      d.getMinutes(),
      d.getSeconds(),
      0
    );
  }

  return null;
}

function toSqlType(col) {
  switch (col.DATA_TYPE) {
    case "datetime2": return sql.DateTime2;
    case "int": return sql.Int;
    case "bigint": return sql.BigInt;
    case "bit": return sql.Bit;
    case "decimal":
    case "numeric": return sql.Decimal(col.NUMERIC_PRECISION ?? 18, col.NUMERIC_SCALE ?? 2);
    case "nvarchar":
      if (col.CHARACTER_MAXIMUM_LENGTH === -1) return sql.NVarChar(sql.MAX);
      return sql.NVarChar(col.CHARACTER_MAXIMUM_LENGTH ?? 255);
    case "sysname": return sql.NVarChar(128);
    default:
      return sql.NVarChar(sql.MAX);
  }
}

function convertByType(sqlTypeName, v) {
  switch (sqlTypeName) {
    case "datetime2": return parseDateSafe(v);
    case "int": return parseIntSafe(v);
    case "bigint": return (v == null || v === "") ? null : BigInt(parseIntSafe(v) ?? 0);
    case "decimal":
    case "numeric": return parseDecimalAR(v);
    case "bit":
      if (v === true || v === 1 || v === "1" || String(v).toLowerCase() === "true") return 1;
      if (v === false || v === 0 || v === "0" || String(v).toLowerCase() === "false") return 0;
      return null;
    default:
      return v == null ? null : String(v);
  }
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export async function runImportMercadoPago({ filePath, originalName }) {
  const schema = env.import.schema; // importacionxls
  const tableName = "Informe Ventas MercadoPago";

  const pool = await getPool();

  // 1) Leer excel
  const { columns: excelCols, rows } = readExcelFirstSheet(filePath);

  // 2) Leer metadata de SQL
  const dbCols = await getTableColumns(schema, tableName);

  const metaCols = ["__ImportedAt", "__SourceFileName", "__RowNumber", "__RowHash"];
  const dbColNames = dbCols.map(c => c.COLUMN_NAME);

  const insertExcelCols = excelCols.filter(c => dbColNames.includes(c));
  const insertCols = [...metaCols, ...insertExcelCols];

  const typeMap = new Map(dbCols.map(c => [c.COLUMN_NAME, c]));

  const batchSize = env.import.batchSize || 2000;
  const batches = chunk(rows, batchSize);

  const now = new Date();

  let attempted = 0;

  // ✅ 0) Count BEFORE (para calcular insert real)
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

      // columnas
      for (const colName of insertCols) {
        const colMeta = typeMap.get(colName);
        const t = colMeta ? toSqlType(colMeta) : sql.NVarChar(sql.MAX);
        const nullable = colMeta ? (colMeta.IS_NULLABLE === "YES") : true;
        part.columns.add(colName, t, { nullable });
      }

      // filas
      const batchRows = batches[b];
      batchRows.forEach((r, idxLocal) => {
        const absoluteRow = b * batchSize + idxLocal;

        const payload = insertExcelCols.map(c => String(r?.[c] ?? "").trim()).join("|");
        const rowHash = sha256Hex(payload);

        const values = [];
        values.push(now);                 // __ImportedAt
        values.push(originalName);        // __SourceFileName
        values.push(absoluteRow + 1);     // __RowNumber
        values.push(rowHash);             // __RowHash

        for (const c of insertExcelCols) {
          const meta = typeMap.get(c);
          const v = convertByType(meta?.DATA_TYPE, r?.[c]);
          values.push(v);
        }

        part.rows.add(...values);
      });

      attempted += batchRows.length;

      // bulk (dup se ignora por IGNORE_DUP_KEY)
      await pool.request().bulk(part);
    }

    // ✅ 7) Count AFTER
    const afterRs = await pool.request()
      .input("File", sql.NVarChar(260), originalName)
      .query(`
        SELECT COUNT(*) AS C
        FROM ${schema}.[${tableName}]
        WHERE __SourceFileName = @File
      `);

    const afterCount = Number(afterRs.recordset?.[0]?.C ?? 0);

    // ✅ insertados reales y duplicados reales
    const insertedRows = Math.max(0, afterCount - beforeCount);
    const skippedDuplicates = Math.max(0, attempted - insertedRows);

    // 8) Registrar ImportRuns (REAL)
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

    // ✅ Respuesta para el front (lo que necesita tu UI)
    return {
      ok: true,
      tableName,
      attemptedRows: attempted,
      insertedRows,
      skippedDuplicates,
      note: "Duplicados se ignoran por índice UNIQUE(__RowHash) con IGNORE_DUP_KEY=ON.",
      // si querés conservar compatibilidad:
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
    try { fs.unlinkSync(filePath); } catch { }
  }
}

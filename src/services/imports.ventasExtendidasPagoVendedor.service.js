import fs from "fs";
import { env } from "../config/env.js";
import { getPool, sql } from "../db/pool.js";
import { readExcelFirstSheet } from "./excel.service.js";
import { getTableColumns } from "./sqlMeta.service.js";
import { sha256Hex } from "../utils/cryptoHash.js";

/* ================== PARSERS ROBUSTOS ================== */

function parseDecimalAR(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;

  let s = String(v).replace(/\u00A0/g, " ").trim();

  let negative = false;
  if (s.startsWith("(") && s.endsWith(")")) {
    negative = true;
    s = s.slice(1, -1).trim();
  }

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
    return ni >= -2147483648 && ni <= 2147483647 ? ni : null;
  }

  let s = String(v).trim();
  if (!s) return null;

  if (s.includes(",") || s.includes(".")) {
    s = s.replace(/\./g, "").replace(",", ".");
    const nDec = Number(s);
    if (!Number.isFinite(nDec)) return null;
    const ni = Math.trunc(nDec);
    return ni >= -2147483648 && ni <= 2147483647 ? ni : null;
  }

  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  const ni = Math.trunc(n);
  return ni >= -2147483648 && ni <= 2147483647 ? ni : null;
}

function parseDateSafe(v) {
  if (!v) return null;

  if (v instanceof Date && !isNaN(v.getTime())) {
    return new Date(
      v.getFullYear(), v.getMonth(), v.getDate(),
      v.getHours(), v.getMinutes(), v.getSeconds(), 0
    );
  }

  const s = String(v).trim();
  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    return new Date(
      Number(m[3]), Number(m[2]) - 1, Number(m[1]),
      Number(m[4] || 0), Number(m[5] || 0), Number(m[6] || 0), 0
    );
  }

  const d = new Date(s);
  if (!isNaN(d.getTime())) {
    return new Date(
      d.getFullYear(), d.getMonth(), d.getDate(),
      d.getHours(), d.getMinutes(), d.getSeconds(), 0
    );
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
      return col.CHARACTER_MAXIMUM_LENGTH === -1
        ? sql.NVarChar(sql.MAX)
        : sql.NVarChar(col.CHARACTER_MAXIMUM_LENGTH ?? 255);
    case "char":
      return sql.Char(col.CHARACTER_MAXIMUM_LENGTH ?? 64);
    default:
      return sql.NVarChar(sql.MAX);
  }
}

function convertByType(type, v) {
  if (type === "datetime2") return parseDateSafe(v);
  if (type === "int") return parseIntSafe(v);
  if (type === "decimal" || type === "numeric") return parseDecimalAR(v);
  return v == null ? null : String(v);
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/* ================== IMPORT ================== */

export async function runImportVentasExtendidasPagoVendedor({ filePath, originalName }) {
  const schema = env.import.schema;
  const tableName = "Ventas extendidas para Pago al Vendedor";

  const pool = await getPool();
  const { columns: excelCols, rows } = readExcelFirstSheet(filePath);
  const dbCols = await getTableColumns(schema, tableName);

  const typeMap = new Map(dbCols.map(c => [c.COLUMN_NAME, c]));
  const insertExcelCols = excelCols.filter(c => typeMap.has(c));

  if (!insertExcelCols.length) {
    try { fs.unlinkSync(filePath); } catch {}
    return {
      ok: true,
      tableName,
      attemptedRows: 0,
      insertedRows: 0,
      skippedDuplicates: 0,
      note: "No matchearon columnas del excel con la tabla."
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
      const t = new sql.Table(`${schema}.${tableName}`);
      t.create = false;

      // ✅ Metacampos SIEMPRE con tipos fijos
      t.columns.add("__ImportedAt", sql.DateTime2, { nullable: false });
      t.columns.add("__SourceFileName", sql.NVarChar(260), { nullable: true });
      t.columns.add("__RowNumber", sql.Int, { nullable: true });
      t.columns.add("__RowHash", sql.Char(64), { nullable: false });

      // ✅ Columnas del excel (tipadas según metadata real)
      for (const c of insertExcelCols) {
        const m = typeMap.get(c);
        const tt = m ? toSqlType(m) : sql.NVarChar(sql.MAX);
        const nullable = m ? (m.IS_NULLABLE === "YES") : true;
        t.columns.add(c, tt, { nullable });
      }

      batches[b].forEach((r, i) => {
        const absoluteRow = b * batchSize + i;

        const payload = insertExcelCols.map(c => String(r[c] ?? "").trim()).join("|");
        const rowHash = sha256Hex(payload);

        const values = [
          now,
          originalName,
          absoluteRow + 1,
          rowHash,
          ...insertExcelCols.map(c => convertByType(typeMap.get(c)?.DATA_TYPE, r[c])),
        ];

        t.rows.add(...values);
      });

      attempted += batches[b].length;
      await pool.request().bulk(t);
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

    // ✅ Response para el front
    return {
      ok: true,
      tableName,
      attemptedRows: attempted,
      insertedRows,
      skippedDuplicates,
      note: "Duplicados se ignoran por índice UNIQUE(__RowHash) con IGNORE_DUP_KEY=ON.",
      insertedRowsApprox: attempted, // opcional compatibilidad
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

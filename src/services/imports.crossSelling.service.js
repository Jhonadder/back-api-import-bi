import fs from "fs";
import { env } from "../config/env.js";
import { getPool, sql } from "../db/pool.js";
import { readExcelFirstSheet } from "./excel.service.js";
import { getTableColumns } from "./sqlMeta.service.js";
import { sha256Hex } from "../utils/cryptoHash.js";

// function parseDecimalAR(v) {
//   if (v === null || v === undefined || v === "") return null;

//   if (typeof v === "number" && Number.isFinite(v)) return v;

//   let s = String(v);
//   s = s.replace(/\u00A0/g, " ").trim();

//   let negative = false;
//   if (s.startsWith("(") && s.endsWith(")")) {
//     negative = true;
//     s = s.slice(1, -1).trim();
//   }

//   s = s.replace(/[^\d,.\- ]/g, "").trim();

//   const hasComma = s.includes(",");
//   const hasDot = s.includes(".");
//   if (hasComma && hasDot) {
//     const lastComma = s.lastIndexOf(",");
//     const lastDot = s.lastIndexOf(".");
//     if (lastComma > lastDot) {
//       s = s.replace(/\./g, "").replace(",", ".");
//     } else {
//       s = s.replace(/,/g, "");
//     }
//   } else if (hasComma) {
//     s = s.replace(/\./g, "").replace(",", ".");
//   } else {
//     s = s.replace(/\s+/g, "");
//   }

//   const n = Number(s);
//   if (!Number.isFinite(n)) return null;

//   return negative ? -n : n;
// }

//incluye funcion para formato contabilidad con signo negativo por NC
function parseDecimalAR(v) {
  if (v === null || v === undefined || v === "") return null;

  // Si ya es número (xlsx muchas veces entrega number), perfecto
  if (typeof v === "number" && Number.isFinite(v)) return v;

  let s = String(v);

  // Normalizar espacios raros (NBSP) y trims
  s = s.replace(/\u00A0/g, " ").trim();

  // Normalizar guiones Unicode a "-"
  // (− U+2212, – U+2013, — U+2014)
  s = s.replace(/[−–—]/g, "-");

  // Caso contabilidad: "(1.234,56)" => negativo
  let negative = false;
  if (s.startsWith("(") && s.endsWith(")")) {
    negative = true;
    s = s.slice(1, -1).trim();
  }

  // Caso contabilidad: "1.234,56-" => negativo
  if (/-\s*$/.test(s)) {
    negative = true;
    s = s.replace(/-\s*$/, "").trim();
  }

  // Quitar moneda/letras y dejar dígitos, separadores, signo y espacios
  // Ej: "-$ 2.295,00" => "- 2.295,00"
  s = s.replace(/[^\d,.\- ]/g, " ").replace(/\s+/g, " ").trim();

  // Si quedó vacío o solo un signo, no hay número real
  if (!s || s === "-" ) return null;

  // Si hay '-' en el medio por el reemplazo anterior, lo sacamos y lo resolvemos con negative
  // Ej: "- 2.295,00" o " - 2.295,00"
  if (s.includes("-")) {
    // si empieza con '-' lo tomamos como negativo
    if (/^\s*-/.test(s)) negative = true;
    s = s.replace(/-/g, "").trim();
  }

  // Normalizar separadores (AR: miles ".", decimal ",")
  const hasComma = s.includes(",");
  const hasDot = s.includes(".");

  if (hasComma && hasDot) {
    // Decide decimal por el último separador
    const lastComma = s.lastIndexOf(",");
    const lastDot = s.lastIndexOf(".");
    if (lastComma > lastDot) {
      // 1.234,56 => 1234.56
      s = s.replace(/\./g, "").replace(",", ".");
    } else {
      // 1,234.56 => 1234.56
      s = s.replace(/,/g, "");
    }
  } else if (hasComma) {
    // 2230,50 => 2230.50
    s = s.replace(/\./g, "").replace(",", ".");
  } else {
    // 2230.50 o 2230
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
    return (ni < -2147483648 || ni > 2147483647) ? null : ni;
  }

  let s = String(v).trim();
  if (!s) return null;

  const hasComma = s.includes(",");
  const hasDot = s.includes(".");
  if (hasComma || hasDot) {
    if (hasComma && hasDot) {
      const lastComma = s.lastIndexOf(",");
      const lastDot = s.lastIndexOf(".");
      if (lastComma > lastDot) s = s.replace(/\./g, "").replace(",", ".");
      else s = s.replace(/,/g, "");
    } else if (hasComma) {
      s = s.replace(/\./g, "").replace(",", ".");
    }
    const nDec = Number(s);
    if (!Number.isFinite(nDec)) return null;
    const ni = Math.trunc(nDec);
    return (ni < -2147483648 || ni > 2147483647) ? null : ni;
  }

  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  const ni = Math.trunc(n);
  return (ni < -2147483648 || ni > 2147483647) ? null : ni;
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
    case "decimal":
    case "numeric": return sql.Decimal(col.NUMERIC_PRECISION ?? 18, col.NUMERIC_SCALE ?? 2);
    case "nvarchar":
      if (col.CHARACTER_MAXIMUM_LENGTH === -1) return sql.NVarChar(sql.MAX);
      return sql.NVarChar(col.CHARACTER_MAXIMUM_LENGTH ?? 255);
    default:
      return sql.NVarChar(sql.MAX);
  }
}

// function convertByType(sqlTypeName, v) {
//   switch (sqlTypeName) {
//     case "datetime2": return parseDateSafe(v);
//     case "int": return parseIntSafe(v);
//     case "decimal":
//     case "numeric": return parseDecimalAR(v);
//     default: return v == null ? null : String(v);
//   }
// }
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
      out = v == null ? null : String(v);
      break;
  }

  // Si el Excel traía algo "visible" y terminó en null => avisar
  const rawHasValue =
    v !== null &&
    v !== undefined &&
    String(v).replace(/\u00A0/g, " ").trim() !== "";

  // if (rawHasValue && (out === null || out === undefined)) {
  //   console.warn(
  //     `[IMPORT WARN] Valor convertido a NULL -> col="${colName}" row=${rowNumber} raw="${String(v)}" sqlType="${sqlTypeName}"`
  //   );
  // }

  return out;
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export async function runImportCrossSelling({ filePath, originalName }) {
  const schema = env.import.schema; // importacionxls
  const tableName = "Ventas CrossSelling";

  const pool = await getPool();

  const { columns: excelCols, rows } = readExcelFirstSheet(filePath);

  const dbCols = await getTableColumns(schema, tableName);
  const dbColNames = dbCols.map(c => c.COLUMN_NAME);
  const typeMap = new Map(dbCols.map(c => [c.COLUMN_NAME, c]));

  const metaCols = ["__ImportedAt", "__SourceFileName", "__RowNumber", "__RowHash"];
  const insertExcelCols = excelCols.filter(c => dbColNames.includes(c));
  const insertCols = [...metaCols, ...insertExcelCols];

  if (!insertExcelCols.length) {
    try { fs.unlinkSync(filePath); } catch { }
    return {
      ok: true,
      tableName,
      attemptedRows: 0,
      insertedRows: 0,
      skippedDuplicates: 0,
      note: "No matchearon columnas del excel con la tabla CrossSelling."
    };
  }

  const batchSize = env.import.batchSize || 2000;
  const batches = chunk(rows, batchSize);

  const now = new Date();
  let attempted = 0;

  // ✅ COUNT BEFORE (por __SourceFileName)
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
        const absoluteRow = b * batchSize + idxLocal;

        const payload = insertExcelCols.map(c => String(r?.[c] ?? "").trim()).join("|");
        const rowHash = sha256Hex(payload);

        const values = [];
        values.push(now);
        values.push(originalName);
        values.push(absoluteRow + 1);
        values.push(rowHash);

        for (const c of insertExcelCols) {
          const meta = typeMap.get(c);
          // const v = convertByType(meta?.DATA_TYPE, r?.[c]);
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

    // ✅ Guardar en ImportRuns REAL
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
      note: "Duplicados se ignoran por RowHash si existe índice UNIQUE con IGNORE_DUP_KEY=ON.",
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
    try { fs.unlinkSync(filePath); } catch { }
  }
}

import fs from "fs";
import { env } from "../config/env.js";
import { readExcel } from "./excel.service.js";
import { sha256Hex } from "../utils/cryptoHash.js";
import { ensureTableFromExcel } from "./sqlTable.service.js";
import { getPool, sql } from "../db/pool.js";
import { fullTable, bracket } from "../utils/sqlIdentifiers.js";

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

export async function importExcelCreateTable({ filePath, originalName }) {
  const schema = env.import.schema;
  const tableName = originalName.replace(/\.(xlsx|xls)$/i, "").trim();
  const { columns, rows } = readExcel(filePath);

  if (!columns.length) {
    try { fs.unlinkSync(filePath); } catch {}
    return { tableName, inserted: 0, message: "Excel sin columnas (header vacío)" };
  }

  await ensureTableFromExcel({ schema, tableName, columns });

  // preparar filas
  const prepared = rows.map((r, idx) => {
    const payload = columns.map((c) => String(r[c] ?? "").trim()).join("|");
    return {
      ...r,
      __ImportedAt: new Date(),
      __SourceFileName: originalName,
      __RowNumber: idx + 1,
      __RowHash: sha256Hex(payload),
    };
  });

  const pool = await getPool();
  const batches = chunk(prepared, env.import.batchSize);

  const tableFull = fullTable(schema, tableName);

  // Insert por lote (parametrizado)
  let inserted = 0;

  for (const b of batches) {
    // Armamos INSERT multi-row
    // Nota: para muchísimas columnas, esto puede crecer; en etapa 2 pasamos a bulk (más rápido).
    const colList = ["__ImportedAt","__SourceFileName","__RowNumber","__RowHash", ...columns];
    const colsSql = colList.map(bracket).join(",");

    const valuesSql = b.map((row, i) => {
      const params = colList.map((_, j) => `@p_${i}_${j}`);
      return `(${params.join(",")})`;
    }).join(",\n");

    const req = pool.request();

    b.forEach((row, i) => {
      colList.forEach((col, j) => {
        const key = `p_${i}_${j}`;
        const val = row[col];
        if (col === "__ImportedAt") req.input(key, sql.DateTime2, val);
        else if (col === "__RowNumber") req.input(key, sql.Int, val);
        else if (col === "__RowHash") req.input(key, sql.Char(64), val);
        else if (col === "__SourceFileName") req.input(key, sql.NVarChar(260), val);
        else req.input(key, sql.NVarChar(sql.MAX), val == null ? "" : String(val));
      });
    });

    await req.query(`INSERT INTO ${tableFull} (${colsSql}) VALUES ${valuesSql};`);
    inserted += b.length; // intentados (duplicados se ignoran por el índice)
  }

  try { fs.unlinkSync(filePath); } catch {}
  return { tableName, inserted, message: "Import OK (duplicados ignorados por RowHash)" };
}

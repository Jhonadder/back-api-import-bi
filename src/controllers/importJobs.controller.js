import fs from "fs";
import path from "path";
import { getPool, sql } from "../db/pool.js";
import { env } from "../config/env.js";
import { runImportMercadoPago } from "../services/imports.mercadopago.service.js"; // y los otros

// 🔥 mapa en memoria para SSE (1 proceso). Luego lo hacemos “multi server” si querés.
const sseClients = new Map(); // jobId -> Set(res)

function sseSend(jobId, payload) {
  const set = sseClients.get(jobId);
  if (!set) return;
  const msg = `data: ${JSON.stringify(payload)}\n\n`;
  for (const res of set) res.write(msg);
}

export async function startImport(req, res) {
  // asumo que ya tenés multer y te llega req.file
  // y que mandás tableName o tipoImport en body
  const { importType } = req.body; // ej "mercadopago" | "volumen" | "jpv" ...
  const file = req.file;

  if (!file) return res.status(400).json({ ok: false, message: "Falta archivo" });
  if (!importType) return res.status(400).json({ ok: false, message: "Falta importType" });

  const pool = await getPool();

  // Tabla destino según importType (mantenelo simple por ahora)
  const map = {
    mercadopago: "Informe Ventas MercadoPago",
    volumen: "Informe Volumen Pago al Vendedor",
    jpv: "JPV",
    serviclub: "Reporte General Serviclub",
    crossselling: "Ventas CrossSelling",
    ventas_extendidas: "Ventas extendidas para Pago al Vendedor",
  };

  const tableName = map[importType];
  if (!tableName) return res.status(400).json({ ok: false, message: "importType inválido" });

  // 1) crear job
  const jobRs = await pool.request()
    .input("TableName", sql.NVarChar(128), tableName)
    .input("SourceFileName", sql.NVarChar(260), file.originalname)
    .query(`
      INSERT INTO dbo.ImportJobs (TableName, SourceFileName, Status)
      OUTPUT inserted.Id
      VALUES (@TableName, @SourceFileName, 'PENDING')
    `);

  const jobId = jobRs.recordset[0].Id;

  // 2) responder rápido
  res.json({ ok: true, jobId, tableName, sourceFileName: file.originalname });

  // 3) ejecutar “en segundo plano” (mismo proceso)
  setImmediate(async () => {
    try {
      sseSend(jobId, { type: "status", status: "RUNNING" });

      let result;
      if (importType === "mercadopago") {
        result = await runImportMercadoPago({ filePath: file.path, originalName: file.originalname, jobId });
      } else if (importType === "volumen") {
        const { runImportVolumenPagoVendedor } = await import("../services/import.volumen.service.js");
        result = await runImportVolumenPagoVendedor({ filePath: file.path, originalName: file.originalname, jobId });
      } else if (importType === "jpv") {
        const { runImportJPV } = await import("../services/import.jpv.service.js");
        result = await runImportJPV({ filePath: file.path, originalName: file.originalname, jobId });
      } else if (importType === "serviclub") {
        const { runImportServiclub } = await import("../services/import.serviclub.service.js");
        result = await runImportServiclub({ filePath: file.path, originalName: file.originalname, jobId });
      } else if (importType === "crossselling") {
        const { runImportCrossSelling } = await import("../services/import.crossselling.service.js");
        result = await runImportCrossSelling({ filePath: file.path, originalName: file.originalname, jobId });
      } else if (importType === "ventas_extendidas") {
        const { runImportVentasExtendidasPagoVendedor } = await import("../services/import.ventas_extendidas.service.js");
        result = await runImportVentasExtendidasPagoVendedor({ filePath: file.path, originalName: file.originalname, jobId });
      }

      sseSend(jobId, { type: "done", result });
    } catch (e) {
      sseSend(jobId, { type: "error", message: e.message });
      // ojo: los services ya registran ImportRuns; acá registramos en ImportJobs también desde los services
    }
  });
}

export async function cancelJob(req, res) {
  const { id } = req.params;
  const pool = await getPool();

  await pool.request()
    .input("Id", sql.UniqueIdentifier, id)
    .query(`
      UPDATE dbo.ImportJobs
      SET CancelRequested = 1
      WHERE Id = @Id
    `);

  sseSend(id, { type: "cancel_requested" });
  res.json({ ok: true });
}

export async function getJob(req, res) {
  const { id } = req.params;
  const pool = await getPool();

  const rs = await pool.request()
    .input("Id", sql.UniqueIdentifier, id)
    .query(`SELECT * FROM dbo.ImportJobs WHERE Id = @Id`);

  if (!rs.recordset.length) return res.status(404).json({ ok: false, message: "Job no existe" });
  res.json({ ok: true, job: rs.recordset[0] });
}

export async function jobEventsSSE(req, res) {
  const { id } = req.params;

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  if (!sseClients.has(id)) sseClients.set(id, new Set());
  sseClients.get(id).add(res);

  // ping inicial
  res.write(`data: ${JSON.stringify({ type: "connected", jobId: id })}\n\n`);

  req.on("close", () => {
    const set = sseClients.get(id);
    if (set) {
      set.delete(res);
      if (!set.size) sseClients.delete(id);
    }
  });
}

import { runImportMercadoPago } from "../services/imports.mercadopago.service.js";
import { runImportVolumenPagoVendedor } from "../services/imports.volumenPagoVendedor.service.js";
import { runImportJPV } from "../services/imports.jpv.service.js";
import { runImportServiclub } from "../services/imports.serviclub.service.js";
import { runImportCrossSelling } from "../services/imports.crossSelling.service.js";
import { runImportVentasExtendidasPagoVendedor } from "../services/imports.ventasExtendidasPagoVendedor.service.js";

export async function importInformeVentasMercadoPago(req, res) {
  if (!req.file?.path) {
    return res.status(400).json({ message: "No se recibió archivo (field: file)" });
  }

  const result = await runImportMercadoPago({
    filePath: req.file.path,
    originalName: req.file.originalname,
    userId: req.user?.sub ?? null,
  });

  return res.json({ ok: true, ...result });
}

export async function importInformeVolumenPagoVendedor(req, res) {
  if (!req.file?.path) {
    return res.status(400).json({ message: "No se recibió archivo (field: file)" });
  }

  const result = await runImportVolumenPagoVendedor({
    filePath: req.file.path,
    originalName: req.file.originalname,
    userId: req.user?.sub ?? null,
  });

  return res.json({ ok: true, ...result });
}

export async function importJPV(req, res) {
  if (!req.file?.path) {
    return res.status(400).json({ message: "No se recibió archivo (field: file)" });
  }

  const result = await runImportJPV({
    filePath: req.file.path,
    originalName: req.file.originalname,
    userId: req.user?.sub ?? null,
  });

  return res.json({ ok: true, ...result });
}

export async function importServiclub(req, res) {
  if (!req.file?.path) {
    return res.status(400).json({ message: "No se recibió archivo (field: file)" });
  }

  const result = await runImportServiclub({
    filePath: req.file.path,
    originalName: req.file.originalname,
    userId: req.user?.sub ?? null,
  });

  return res.json({ ok: true, ...result });
}

export async function importCrossSelling(req, res) {
  if (!req.file?.path) {
    return res.status(400).json({ message: "No se recibió archivo (field: file)" });
  }

  const result = await runImportCrossSelling({
    filePath: req.file.path,
    originalName: req.file.originalname,
  });

  return res.json({ ok: true, ...result });
}

export async function importVentasExtendidasPagoVendedor(req, res) {
  if (!req.file?.path) {
    return res.status(400).json({ message: "No se recibió archivo" });
  }

  const result = await runImportVentasExtendidasPagoVendedor({
    filePath: req.file.path,
    originalName: req.file.originalname,
  });

  res.json({ ok: true, ...result });
}

import { Router } from "express";
import { requireAuth } from "../middlewares/auth.middleware.js";
import { uploadExcel } from "../middlewares/upload.middleware.js";
import { importInformeVentasMercadoPago } from "../controllers/imports.controller.js";
import { importInformeVolumenPagoVendedor } from "../controllers/imports.controller.js";
import { importJPV } from "../controllers/imports.controller.js";
import { importServiclub } from "../controllers/imports.controller.js";
import { importCrossSelling } from "../controllers/imports.controller.js";
import { importVentasExtendidasPagoVendedor } from "../controllers/imports.controller.js";
import { listImportRuns } from "../controllers/importRuns.controller.js";

// ...

const router = Router();

router.post(
  "/mercadopago",
  requireAuth,
  (req, res) =>
    uploadExcel(req, res, (err) => {
      if (err) return res.status(400).json({ message: err.message });
      return importInformeVentasMercadoPago(req, res);
    })
);

router.post(
  "/volumen-pago-vendedor",
  requireAuth,
  (req, res) =>
    uploadExcel(req, res, (err) => {
      if (err) return res.status(400).json({ message: err.message });
      return importInformeVolumenPagoVendedor(req, res);
    })
);

router.post(
  "/jpv",
  requireAuth,
  (req, res) =>
    uploadExcel(req, res, (err) => {
      if (err) return res.status(400).json({ message: err.message });
      return importJPV(req, res);
    })
);

router.post(
  "/serviclub",
  requireAuth,
  (req, res) =>
    uploadExcel(req, res, (err) => {
      if (err) return res.status(400).json({ message: err.message });
      return importServiclub(req, res);
    })
);

router.post(
  "/cross-selling",
  requireAuth,
  (req, res) =>
    uploadExcel(req, res, (err) => {
      if (err) return res.status(400).json({ message: err.message });
      return importCrossSelling(req, res);
    })
);

router.post(
  "/ventas-extendidas-pago-vendedor",
  requireAuth,
  (req, res) =>
    uploadExcel(req, res, err => {
      if (err) return res.status(400).json({ message: err.message });
      return importVentasExtendidasPagoVendedor(req, res);
    })
);

router.get(
  "/runs",
  requireAuth,
  listImportRuns
);

export default router;

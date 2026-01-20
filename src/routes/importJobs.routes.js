import { Router } from "express";
import {
  startImport,
  cancelJob,
  getJob,
  jobEventsSSE
} from "../controllers/importJobs.controller.js";

const router = Router();

router.post("/start", startImport);               // devuelve jobId
router.post("/:id/cancel", cancelJob);            // marca cancelRequested=1
router.get("/:id", getJob);                       // polling (opcional)
router.get("/:id/events", jobEventsSSE);          // SSE (progreso en vivo)

export default router;

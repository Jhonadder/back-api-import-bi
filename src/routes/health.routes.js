import { Router } from "express";
const router = Router();
router.get("/", (req, res) => res.json({ ok: true, api:"api-import-bi", time: new Date().toISOString() }));
console.log("consultando Health");
export default router;

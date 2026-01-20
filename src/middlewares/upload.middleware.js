import fs from "fs";
import path from "path";
import multer from "multer";
import { env } from "../config/env.js";

const uploadDir = path.resolve(process.cwd(), env.upload.dir);
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const safe = file.originalname.replace(/[^\w.\- ()]/g, "_");
    cb(null, `${Date.now()}__${safe}`);
  },
});

function fileFilter(req, file, cb) {
  const ok = /\.(xlsx|xls)$/i.test(file.originalname);
  if (!ok) return cb(new Error("Solo .xlsx/.xls"));
  cb(null, true);
}

const limits =
  env.upload.maxFileSizeMb > 0
    ? { fileSize: env.upload.maxFileSizeMb * 1024 * 1024 }
    : undefined;

export const uploadExcel = multer({ storage, fileFilter, limits }).single("file");

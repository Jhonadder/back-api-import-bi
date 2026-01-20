import express from "express";
import cors from "cors";

import healthRoutes from "./routes/health.routes.js";
import authRoutes from "./routes/auth.routes.js";
import usersRoutes from "./routes/users.routes.js";
import importsRoutes from "./routes/imports.routes.js";
import { errorMiddleware } from "./middlewares/error.middleware.js";
import importJobsRoutes from "./routes/importJobs.routes.js";

export function createApp() {
  const app = express();
  app.use(cors());
  app.use(express.json());

  app.use("/api/health", healthRoutes);
  app.use("/api/auth", authRoutes);
  app.use("/api/users", usersRoutes);
  app.use("/api/imports", importsRoutes);
  app.use("/api/import-jobs", importJobsRoutes);

  app.use(errorMiddleware);
  return app;
}

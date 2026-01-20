import { Router } from "express";
import { requireAuth } from "../middlewares/auth.middleware.js";
import {
  listUsers, getUser, createUser, updateUser, deleteUser,
} from "../controllers/users.controller.js";

const router = Router();

router.use(requireAuth);
router.get("/", listUsers);
router.get("/:id", getUser);
router.post("/", createUser);
router.put("/:id", updateUser);
router.delete("/:id", deleteUser);

export default router;

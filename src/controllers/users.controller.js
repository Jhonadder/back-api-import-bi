import bcrypt from "bcrypt";
import { HttpError } from "../utils/httpError.js";
import {
  getUsers, getUserById, createUserDb, updateUserDb, deleteUserDb,
} from "../services/users.service.js";

export async function listUsers(req, res, next) {
  try { res.json(await getUsers()); } catch (e) { next(e); }
}

export async function getUser(req, res, next) {
  try {
    const user = await getUserById(Number(req.params.id));
    if (!user) throw new HttpError(404, "No existe");
    res.json(user);
  } catch (e) { next(e); }
}

export async function createUser(req, res, next) {
  try {
    const { username, password, isActive = true } = req.body;
    if (!username || !password) throw new HttpError(400, "username y password requeridos");
    const hash = await bcrypt.hash(password, 10);
    const created = await createUserDb({ username, passwordHash: hash, isActive });
    res.status(201).json(created);
  } catch (e) { next(e); }
}

export async function updateUser(req, res, next) {
  try {
    const id = Number(req.params.id);
    const { username, password, isActive } = req.body;

    const patch = {};
    if (username) patch.username = username;
    if (typeof isActive === "boolean") patch.isActive = isActive;
    if (password) patch.passwordHash = await bcrypt.hash(password, 10);

    const updated = await updateUserDb(id, patch);
    if (!updated) throw new HttpError(404, "No existe");
    res.json(updated);
  } catch (e) { next(e); }
}

export async function deleteUser(req, res, next) {
  try {
    const ok = await deleteUserDb(Number(req.params.id));
    if (!ok) throw new HttpError(404, "No existe");
    res.status(204).send();
  } catch (e) { next(e); }
}

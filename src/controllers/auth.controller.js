import jwt from "jsonwebtoken";
import bcrypt from "bcrypt";
import { env } from "../config/env.js";
import { findUserByUsername } from "../services/users.service.js";
import { HttpError } from "../utils/httpError.js";

export async function login(req, res, next) {
  try {
    const { username, password } = req.body;
    if (!username || !password) throw new HttpError(400, "username y password son requeridos");

    const user = await findUserByUsername(username);
    if (!user) throw new HttpError(401, "Credenciales inválidas");

    const ok = await bcrypt.compare(password, user.PasswordHash);
    if (!ok) throw new HttpError(401, "Credenciales inválidas");

    const token = jwt.sign(
      { sub: user.Id, username: user.Username },
      env.jwt.secret,
      { expiresIn: env.jwt.expiresIn }
    );

    res.json({ token });
  } catch (e) {
    next(e);
  }
}

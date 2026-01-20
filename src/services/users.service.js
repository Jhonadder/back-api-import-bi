import { getPool, sql } from "../db/pool.js";

export async function getUsers() {
  const pool = await getPool();
  const rs = await pool.request().query(`
    SELECT Id, Username, IsActive, CreatedAt
    FROM dbo.Users
    ORDER BY Id DESC
  `);
  return rs.recordset;
}

export async function getUserById(id) {
  const pool = await getPool();
  const rs = await pool.request().input("Id", sql.Int, id).query(`
    SELECT Id, Username, IsActive, CreatedAt
    FROM dbo.Users WHERE Id = @Id
  `);
  return rs.recordset[0] || null;
}

export async function findUserByUsername(username) {
  const pool = await getPool();
  const rs = await pool.request().input("Username", sql.NVarChar(120), username).query(`
    SELECT TOP 1 * FROM dbo.Users WHERE Username = @Username AND IsActive = 1
  `);
  return rs.recordset[0] || null;
}

export async function createUserDb({ username, passwordHash, isActive }) {
  const pool = await getPool();
  const rs = await pool.request()
    .input("Username", sql.NVarChar(120), username)
    .input("PasswordHash", sql.NVarChar(255), passwordHash)
    .input("IsActive", sql.Bit, isActive ? 1 : 0)
    .query(`
      INSERT INTO dbo.Users (Username, PasswordHash, IsActive)
      OUTPUT INSERTED.Id, INSERTED.Username, INSERTED.IsActive, INSERTED.CreatedAt
      VALUES (@Username, @PasswordHash, @IsActive)
    `);
  return rs.recordset[0];
}

export async function updateUserDb(id, patch) {
  const pool = await getPool();

  const sets = [];
  const req = pool.request().input("Id", sql.Int, id);

  if (patch.username) { sets.push("Username=@Username"); req.input("Username", sql.NVarChar(120), patch.username); }
  if (typeof patch.isActive === "boolean") { sets.push("IsActive=@IsActive"); req.input("IsActive", sql.Bit, patch.isActive ? 1 : 0); }
  if (patch.passwordHash) { sets.push("PasswordHash=@PasswordHash"); req.input("PasswordHash", sql.NVarChar(255), patch.passwordHash); }

  if (!sets.length) return await getUserById(id);

  const rs = await req.query(`
    UPDATE dbo.Users SET ${sets.join(", ")}
    OUTPUT INSERTED.Id, INSERTED.Username, INSERTED.IsActive, INSERTED.CreatedAt
    WHERE Id=@Id
  `);

  return rs.recordset[0] || null;
}

export async function deleteUserDb(id) {
  const pool = await getPool();
  const rs = await pool.request().input("Id", sql.Int, id).query(`
    DELETE FROM dbo.Users WHERE Id=@Id
  `);
  return rs.rowsAffected[0] > 0;
}

import { getPool, sql } from "../db/pool.js";

export async function getTableColumns(schema, tableName) {
  const pool = await getPool();

  const rs = await pool.request()
    .input("Schema", sql.NVarChar(128), schema)
    .input("Table", sql.NVarChar(128), tableName)
    .query(`
      SELECT
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.CHARACTER_MAXIMUM_LENGTH,
        c.NUMERIC_PRECISION,
        c.NUMERIC_SCALE,
        c.IS_NULLABLE,
        c.ORDINAL_POSITION
      FROM INFORMATION_SCHEMA.COLUMNS c
      WHERE c.TABLE_SCHEMA = @Schema
        AND c.TABLE_NAME = @Table
      ORDER BY c.ORDINAL_POSITION ASC;
    `);

  return rs.recordset;
}

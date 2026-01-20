import { getPool, sql } from "../db/pool.js";

export async function getImportRuns({
    from,
    to,
    tableName,
    status,
    limit = 100,
}) {
    const pool = await getPool();

    let where = "WHERE 1=1";
    const req = pool.request();

      if (from) {
        where += " AND ImportedAt >= @From";
        req.input("From", sql.DateTime2, from);
      }

      if (to) {
        where += " AND ImportedAt <= @To";
        req.input("To", sql.DateTime2, to);
      }
    // if (from) {
    //     // Si 'from' es un objeto Date, lo convertimos a string local antes de enviarlo
    //     const fromStr = from instanceof Date ? from.toISOString().replace('T', ' ').split('.')[0] : from;

    //     where += " AND ImportedAt >= @From";
    //     req.input("From", sql.DateTime2, fromStr);
    // }

    // if (to) {
    //     const toStr = to instanceof Date ? to.toISOString().replace('T', ' ').split('.')[0] : to;

    //     where += " AND ImportedAt <= @To";
    //     req.input("To", sql.DateTime2, toStr);
    // }

    if (tableName) {
        where += " AND TableName = @TableName";
        req.input("TableName", sql.NVarChar(128), tableName);
    }

    if (status) {
        where += " AND Status = @Status";
        req.input("Status", sql.NVarChar(20), status);
    }

    req.input("Limit", sql.Int, limit);

    const result = await req.query(`
    SELECT TOP (@Limit)
      Id,
      TableName,
      SourceFileName,
      CONVERT(varchar, ImportedAt, 126) as ImportedAt,
      InsertedRows,
      SkippedDuplicates,
      Status,
      ErrorMessage
    FROM dbo.ImportRuns
    ${where}
    ORDER BY ImportedAt DESC
  `);

    return result.recordset;
}

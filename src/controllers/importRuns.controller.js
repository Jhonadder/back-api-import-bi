import { getImportRuns } from "../services/importRuns.service.js";

export async function listImportRuns(req, res) {
  const {
    from,
    to,
    tableName,
    status,
    limit,
  } = req.query;

  const data = await getImportRuns({
    from: from ? new Date(from) : null,
    to: to ? new Date(to) : null,
    tableName,
    status,
    limit: limit ? Number(limit) : 100,
  });

  res.json({
    ok: true,
    count: data.length,
    data,
  });
}

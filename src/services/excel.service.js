import xlsx from "xlsx";

export function readExcelFirstSheet(filePath) {
  const wb = xlsx.readFile(filePath, {
    cellDates: true,  // clave para fechas
    raw: false,
  });

  const sheetName = wb.SheetNames[0];
  const ws = wb.Sheets[sheetName];

  const rows = xlsx.utils.sheet_to_json(ws, {
    defval: null,     // mejor null que ""
    raw: false,
  });

  const columns = rows.length ? Object.keys(rows[0]) : [];
  return { sheetName, columns, rows };
}

export function bracket(name) {
  // escapa ] duplicándolo (SQL Server)
  return `[${String(name).replace(/]/g, "]]")}]`;
}

export function fullTable(schema, table) {
  return `${bracket(schema)}.${bracket(table)}`;
}

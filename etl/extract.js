// --- Extraction ---
export async function extract() {
  const tableNames = [
    "registro_actividad",
    "equipo",
    "usuario",
    "obra",
    "precio_etiqueta",
    "etiquetas_equipos",
    "viaje",
    "horarios_obras",
    "historico_salario",
    "destino",
  ];

  const extractedTablesArray = await Promise.all(
    tableNames.map((name) => extractOne(name))
  );

  const tables = {};
  tableNames.forEach((name, i) => {
    const data = extractedTablesArray[i];
    tables[name] = data;
  });

  if (!tables.registro_actividad || tables.registro_actividad.length === 0) {
    console.warn(
      "⚠️ No registro records found. ETL will proceed but fact table might be empty or partial."
    );
  }
  return tables;
}

async function extractOne(tableName) {
  const appId = process.env.APP_ID;
  const appKey = process.env.APP_KEY;
  if (!appId || !appKey) {
    throw new Error("APP_ID and APP_KEY environment variables must be set.");
  }
  const url = `https://www.appsheet.com/api/v2/apps/${appId}/tables/${tableName}/Action`;

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        ApplicationAccessKey: appKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ Action: "Find" }),
    });

    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(
        `Failed to fetch ${tableName}: ${response.status} - ${errorBody}`
      );
    }
    return await response.json();
  } catch (e) {
    console.error(`❌ Error extracting ${tableName}:`, e.message);
    return [];
  }
}

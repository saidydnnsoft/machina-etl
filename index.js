import functions from "@google-cloud/functions-framework";
import { BigQuery } from "@google-cloud/bigquery";
import dotenv from "dotenv";
import fs from "fs";
import {
  getDate,
  getYear,
  startOfMonth,
  startOfQuarter,
  startOfWeek,
  startOfYear,
} from "date-fns";

if (fs.existsSync(".env")) {
  dotenv.config();
}

import path from "path";
import os from "os";

// --- Main HTTP Function ---
functions.http("runEtl", async (req, res) => {
  if (req.method !== "POST") {
    return res.status(405).send("Method Not Allowed");
  }
  try {
    console.log("🚀 Starting ETL process...");
    const rawTables = await extract();
    console.log("✅ Extraction complete.");

    const transformedData = transform(rawTables);
    console.log("✅ Transformation complete.");
    for (const key in transformedData) {
      if (transformedData[key]) {
        console.log(`Transformed ${key} count: ${transformedData[key].length}`);
      }
    }

    // await load_all_data(transformedData);
    res.send("✅ ETL complete!");
  } catch (error) {
    console.error("❌ ETL failed:", error.message);
    if (error.stack) console.error(error.stack);
    if (error.errors) {
      error.errors.forEach((err) =>
        console.error(
          `BQ Error: ${err.message}, Reason: ${err.reason}, Location: ${err.location}`
        )
      );
    }
    res.status(500).send(`ETL failed: ${error.message}`);
  }
});

// --- Extraction ---
async function extract() {
  const tableNames = [
    "registro_actividad",
    "equipo",
    "usuario",
    "obra",
    "precio_etiqueta",
    "etiquetas_equipos",
    "viaje",
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

// --- Transformation ---
function formatToBQDate(dateStr) {
  if (!dateStr) return null;
  const datePart = dateStr.split(" ")[0];
  const [month, day, year] = datePart.split("/");
  if (!year || !month || !day) return null;
  return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
}

function transform_dim_obra(obras_arr) {
  if (!obras_arr) return [];
  return obras_arr.map((o) => ({
    id_obra: o["Row ID"],
    nombre_obra: o.nombre_obra ?? null,
  }));
}

function transform_dim_usuario(usuarios_arr) {
  if (!usuarios_arr) return [];
  return usuarios_arr.map((u) => ({
    id_usuario: u["Row ID"],
    usuario: u.usuario ?? null,
  }));
}

function transform_dim_equipo(equipos_arr, etiquetas_equipos_arr) {
  const etiquetas_equipos_map = new Map(
    etiquetas_equipos_arr.map((et) => [et["Row ID"], et])
  );
  if (!equipos_arr) return [];
  return equipos_arr.map((e) => ({
    id_equipo: e["Row ID"],
    codigo_interno: e.codigo_interno ?? null,
    descripcion: e.descripcion ?? null,
    tipo_equipo: e.tipo_equipo ?? null,
    tipo_activo: e.tipo_activo ?? null,
    etiqueta_equipo:
      etiquetas_equipos_map.get(e.id_etiqueta_equipo)?.etiqueta ?? null,
  }));
}

function transform_dim_fecha(uniqueDateStringsSet) {
  const days = [
    "Domingo",
    "Lunes",
    "Martes",
    "Miércoles",
    "Jueves",
    "Viernes",
    "Sábado",
  ];
  const months = [
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
  ];
  const dimDateRows = [];

  for (const dateStr of uniqueDateStringsSet) {
    if (!dateStr) continue;

    const dateObj = new Date(dateStr + "T00:00:00Z");

    const year = dateObj.getUTCFullYear();
    const month = dateObj.getUTCMonth() + 1;
    const day = dateObj.getUTCDate();
    const dayOfWeek = dateObj.getUTCDay();

    dimDateRows.push({
      id_fecha: dateStr,
      fecha: `${getDate(dateObj).toString().padStart(2, "0")}/${(month + 1)
        .toString()
        .padStart(2, "0")}/${getYear(dateObj)}`,
      anio: year,
      num_mes: month,
      nombre_mes: months[month - 1],
      num_dia: day,
      num_dia_semana: dayOfWeek,
      nombre_dia: days[dayOfWeek],
      trimestre: `Q${Math.floor((month - 1) / 3) + 1}`,
      inicio_mes: startOfMonth(dateObj).toISOString().split("T")[0],
      inicio_anio: startOfYear(dateObj).toISOString().split("T")[0],
      inicio_trimestre: startOfQuarter(dateObj).toISOString().split("T")[0],
      inicio_semana: startOfWeek(dateObj).toISOString().split("T")[0],
    });
  }
  return dimDateRows;
}

// Fact Transformer
function transform_fact_produccion(rawData) {
  const {
    registro_actividad_arr,
    equipo_arr,
    usuario_arr,
    obra_arr,
    precio_etiqueta_arr,
    etiquetas_equipos_arr,
    viaje_arr,
  } = rawData;

  const equipos_map = new Map(equipo_arr.map((e) => [e["Row ID"], e]));
  const viajes_map = new Map(viaje_arr.map((v) => [v["Row ID"], v]));
  const precios_etiquetas_map = new Map();
  precio_etiqueta_arr.forEach((p) => {
    const key = `${p.id_obra}-${p.id_etiqueta_equipo}-${p.unidad_de_medida}${
      p.id_destino ? `-${p.id_destino}` : ""
    }`;
    if (!precios_etiquetas_map.has(key)) {
      precios_etiquetas_map.set([p]);
    } else {
      precios_etiquetas_map.set(key, [...precios_etiquetas_map.get(key), p]);
    }
  });

  const registros = (registro_actividad_arr || [])
    .map((registro) => {
      // necesito buscar el tipo activo del equipo de acuerdo al id_equipo
      // necesito ir a buscar los related viajes en la tabla viaje
      // del viaje necesito saber el destino
      // necesito la etiqueta del equipo
      // necesito ir a buscar el precio del equipo en esta obra, para la etiqueta de acuerdo a la fecha
      // valor activo EQUIPO - HORA
      // +
      // valor activo VEHICULO - DIA
      // +
      // valor viajes - RELATED VIAJES
      return {
        id_registro: registro["Row ID"] ?? null,
        id_equipo: registro.id_equipo ?? null,
        id_operador: registro.operador ?? null,
        id_responsable_de_obra: registro.responsable_de_obra ?? null,
        id_obra: registro.id_obra ?? null,
        id_fecha: registro.hora_inicial
          ? formatToBQDate(registro.hora_inicial)
          : null,
        estado: registro.estado ?? null,
        estado_aprobacion: registro.estado_aprobacion ?? null,
        varado: registro.varado === "Y" ? "Sí" : "No",
        horas_trabajadas: parseFloat(registro.horas_trabajadas_maquina || 0),
        kilometros_recorridos: parseFloat(registro.kilometros_recorridos || 0),
        combustible: parseFloat(registro.combustible || 0),
        horas_varado: parseFloat(registro.horas_varado || 0),
        heod: parseFloat(registro.heod || 0),
        heon: parseFloat(registro.heon || 0),
        hefd: parseFloat(registro.hefd || 0),
        hefn: parseFloat(registro.hefn || 0),
        rno: parseFloat(registro.rno || 0),
        rnf: parseFloat(registro.rnf || 0),
        hf: parseFloat(registro.hf || 0),
        valor_extras_y_recargos: parseFloat(
          registro.valor_extras_y_recargos_vc || 0
        ),
        valor_activo: parseFloat(registro.valor_activo_vc || 0),
        num_viajes: parseFloat(registro.num_viajes_vc || 0),
      };
    })
    .filter(Boolean);

  return registros;
}

function transform(tables) {
  const transformed_data = {};

  // --- 1. Prepare Date Dimension ---
  const uniqueDateStrings = new Set();
  if (tables.registro_actividad) {
    tables.registro_actividad.forEach((r) => {
      if (r.hora_inicial) {
        const formattedDate = formatToBQDate(r.hora_inicial);
        if (formattedDate) uniqueDateStrings.add(formattedDate);
      }
      if (r.hora_inicial) {
        const formattedDate = formatToBQDate(r.hora_inicial);
        if (formattedDate) uniqueDateStrings.add(formattedDate);
      }
    });
  }
  transformed_data.dim_fecha = transform_dim_fecha(uniqueDateStrings);

  // --- 2. Transform Other Dimensions ---
  transformed_data.dim_equipo = transform_dim_equipo(
    tables.equipo,
    tables.etiquetas_equipos
  );
  transformed_data.dim_obra = transform_dim_obra(tables.obra);
  transformed_data.dim_usuario = transform_dim_usuario(tables.usuario);

  // --- 3. Transform Facts ---
  transformed_data.fact_produccion = transform_fact_produccion(tables);
  return transformed_data;
}

// --- Loading ---
async function load_table_data(bq, datasetId, tableId, rows) {
  if (!rows || rows.length === 0) {
    console.log(`ℹ️ No rows to load into ${datasetId}.${tableId}. Skipping.`);
    return;
  }
  if (!datasetId || !tableId) {
    const errorMessage = `Missing datasetId or tableId for loading. Dataset: ${datasetId}, Table: ${tableId}. Skipping.`;
    console.error(`❌ ${errorMessage}`);
    throw new Error(errorMessage);
  }

  const table = bq.dataset(datasetId).table(tableId);
  const tmpFileName = `data-${tableId.replace(/_/g, "-")}-${Date.now()}.ndjson`;
  const tmpFile = path.join(os.tmpdir(), tmpFileName);

  fs.writeFileSync(tmpFile, rows.map((r) => JSON.stringify(r)).join("\n"));

  const metadata = {
    sourceFormat: "NEWLINE_DELIMITED_JSON",
    writeDisposition: "WRITE_TRUNCATE",
  };

  let job;

  try {
    [job] = await table.load(tmpFile, metadata);
    console.log(
      `🚀 Load job for ${tableId} (ID: ${job.id}) initiated. Checking initial status...`
    );

    if (job?.status?.errors) {
      console.error(
        `❌ Load job ${job.id} for ${tableId} reported errors upon initiation:`
      );
      job.status.errors.forEach((err) =>
        console.error(
          `  Message: ${err.message}, Reason: ${err.reason}, Location: ${err.location}`
        )
      );
      const errorMessages = job.status.errors
        .map(
          (e) => `${e.reason}: ${e.message} (Location: ${e.location || "N/A"})`
        )
        .join("; ");
      throw new Error(
        `BigQuery load for ${tableId} (Job ID: ${job.id}) failed with errors: ${errorMessages}`
      );
    }

    if (job?.status?.state === "DONE") {
      console.log(
        `✅ BigQuery load job ${job.id} for ${datasetId}.${tableId} reported as DONE immediately.`
      );
    } else {
      console.warn(
        `⚠️ Load job ${job.id} for ${tableId} initiated. Current state: ${
          job?.status?.state || "UNKNOWN"
        }. Full status:`,
        job?.status
      );
    }
  } catch (error) {
    const jobIdInfo = job?.id ? `(Job ID: ${job.id}) ` : "(Job ID: UNKNOWN) ";
    console.error(
      `❌ Error during BigQuery load initiation for ${datasetId}.${tableId} ${jobIdInfo}: ${error.message}`
    );
    if (error.errors) {
      error.errors.forEach((err) =>
        console.error(`  API Error Detail: ${err.message}`)
      );
    }
    throw error;
  } finally {
    try {
      if (fs.existsSync(tmpFile)) {
        fs.unlinkSync(tmpFile);
      }
    } catch (unlinkErr) {
      console.warn(`⚠️ Failed to delete temp file ${tmpFile}:`, unlinkErr);
    }
  }
}

async function load_all_data(transformedData) {
  const datasetId = process.env.DATASET_ID;
  if (!datasetId) {
    throw new Error("DATASET_ID environment variable must be set.");
  }
  let bq;
  if (fs.existsSync("service-account-key.json")) {
    bq = new BigQuery({
      keyFilename: "service-account-key.json",
    });
  } else {
    bq = new BigQuery();
  }

  // Define table IDs from environment variables
  const tableIds = {
    dim_obra: process.env.DIM_OBRA_TABLE_ID,
    dim_equipo: process.env.DIM_EQUIPO_TABLE_ID,
    dim_usuario: process.env.DIM_USUARIO_TABLE_ID,
    dim_fecha: process.env.DIM_FECHA_TABLE_ID,
    fact_produccion: process.env.FACT_PRODUCCION_TABLE_ID,
  };

  // Validate all table IDs are present
  for (const key in tableIds) {
    if (!tableIds[key]) {
      console.warn(
        `⚠️ Environment variable for ${key.toUpperCase()}_TABLE_ID is not set. Skipping this table.`
      );
    }
  }

  // Load dimensions first
  if (tableIds.dim_equipo)
    await load_table_data(
      bq,
      datasetId,
      tableIds.dim_equipo,
      transformedData.dim_equipo
    );
  if (tableIds.dim_obra)
    await load_table_data(
      bq,
      datasetId,
      tableIds.dim_obra,
      transformedData.dim_obra
    );
  if (tableIds.dim_usuario)
    await load_table_data(
      bq,
      datasetId,
      tableIds.dim_usuario,
      transformedData.dim_usuario
    );
  if (tableIds.dim_fecha)
    await load_table_data(
      bq,
      datasetId,
      tableIds.dim_fecha,
      transformedData.dim_fecha
    );

  // Load fact tables
  if (
    tableIds.fact_produccion &&
    transformedData.fact_produccion &&
    transformedData.fact_produccion.length > 0
  ) {
    await load_table_data(
      bq,
      datasetId,
      tableIds.fact_produccion,
      transformedData.fact_produccion
    );
  }

  console.log("✅ All loading tasks initiated.");
}

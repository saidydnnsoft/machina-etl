import { BigQuery } from "@google-cloud/bigquery";
import fs from "fs";
import path from "path";
import os from "os";

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

export async function load(transformedData) {
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

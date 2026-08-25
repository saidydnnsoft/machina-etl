import ExcelJS from "exceljs";
import { getBogotaDateString } from "./utils.js";
import { CONCEPTOS_BITAKORA } from "../calculadora-extras/bitakora.js";

// Marca registros como procesados poniendo procesado_rrhh="Y" en registro_actividad,
// mediante bulk Edit (muchos Rows por POST) en vez de 1 POST por registro. Así la
// marca queda visible en la tabla operativa y el job termina en ~segundos.
async function updateRegistrosProcesadoRRHH(registroIds, maxRetries = 5) {
  const appId = process.env.APP_ID;
  const appKey = process.env.APP_KEY;

  if (!appId || !appKey) {
    console.warn(
      "⚠️ APP_ID and APP_KEY not configured. Skipping AppSheet updates.",
    );
    return { successful: [], failed: [] };
  }

  const url = `https://www.appsheet.com/api/v2/apps/${appId}/tables/registro_actividad/Action`;

  // Timeout por request: abortamos y reintentamos si una request se cuelga, en
  // vez de esperar indefinidamente.
  const FETCH_TIMEOUT_MS = parseInt(
    process.env.APPSHEET_FETCH_TIMEOUT_MS || "60000",
    10,
  );

  // Edita un chunk completo en una sola llamada, con retry + backoff exponencial.
  const editChunk = async (chunk) => {
    let lastError = null;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
      try {
        const response = await fetch(url, {
          method: "POST",
          headers: {
            ApplicationAccessKey: appKey,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            Action: "Edit",
            Properties: {
              Locale: "en-US",
            },
            Rows: chunk.map((registroId) => ({
              "Row ID": registroId,
              procesado_rrhh: "Y",
            })),
          }),
          signal: controller.signal,
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${await response.text()}`);
        }

        console.log(
          `✅ Marcados ${chunk.length} registros como procesado_rrhh`,
        );
        return { success: true, ids: chunk };
      } catch (error) {
        lastError = error;
        console.warn(
          `⚠️ Attempt ${attempt}/${maxRetries} failed for chunk of ${chunk.length}: ${error.message}`,
        );

        if (attempt < maxRetries) {
          // Wait before retrying (exponential backoff, cap 20s)
          const waitTime = Math.min(1000 * Math.pow(2, attempt - 1), 20000);
          await new Promise((resolve) => setTimeout(resolve, waitTime));
        }
      } finally {
        clearTimeout(timeoutId);
      }
    }

    console.error(
      `❌ Failed to mark chunk of ${chunk.length} after ${maxRetries} attempts:`,
      lastError.message,
    );
    return { success: false, ids: chunk, error: lastError.message };
  };

  // 1 POST por chunk (no por registro). Tuneable por env.
  const CHUNK_SIZE = parseInt(process.env.APPSHEET_CHUNK_SIZE || "50", 10);
  const CHUNK_DELAY_MS = parseInt(
    process.env.APPSHEET_CHUNK_DELAY_MS || "200",
    10,
  );

  const successful = [];
  const failed = [];

  for (let i = 0; i < registroIds.length; i += CHUNK_SIZE) {
    const chunk = registroIds.slice(i, i + CHUNK_SIZE);
    const chunkNumber = Math.floor(i / CHUNK_SIZE) + 1;
    const totalChunks = Math.ceil(registroIds.length / CHUNK_SIZE);

    console.log(
      `🔄 Processing chunk ${chunkNumber}/${totalChunks} (${chunk.length} records)...`,
    );

    const result = await editChunk(chunk);
    if (result.success) {
      successful.push(...result.ids);
    } else {
      failed.push(...result.ids);
    }

    // Espera entre chunks (excepto el último) si se configuró un delay.
    if (i + CHUNK_SIZE < registroIds.length && CHUNK_DELAY_MS > 0) {
      await new Promise((resolve) => setTimeout(resolve, CHUNK_DELAY_MS));
    }
  }

  return { successful, failed };
}

export function generateFortnightReportData(transformedData) {
  const { fact_produccion } = transformedData;
  const reportData = [];

  // Process each record to extract the required fields
  fact_produccion.forEach((record) => {
    reportData.push({
      obra: record.id_obra,
      operador: record.id_operador,
      heod: record.heod,
      heon: record.heon,
      hefd: record.hefd,
      hefn: record.hefn,
      rno: record.rno,
      rnf: record.rnf,
      hf: record.hf,
      // We need to get the actual time values from the registro_actividad
      // For now, we'll leave these as placeholders
      hora_inicial: null,
      hora_final: null,
      hora_inicio_descanso: null,
      hora_fin_descanso: null,
    });
  });

  return reportData;
}

export async function generateMonthlyReportExcel(
  reportData,
  rawTables,
  dimObra,
  dimUsuario,
) {
  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet("Reporte Quincenal");

  // Create maps for lookups
  const obraMap = new Map(
    rawTables.obra.map((o) => [
      o["Row ID"],
      { nombre: o.nombre_obra, ciudad: o.ciudad || "" },
    ]),
  );
  // Use raw usuario table to get nombre, apellido, and identificacion fields
  const usuarioMap = new Map(
    rawTables.usuario.map((u) => [
      u["Row ID"],
      {
        nombre: u.nombre || u.usuario,
        apellido: u.apellido || "",
        identificacion: u.identificacion || "",
      },
    ]),
  );
  const registroMap = new Map(
    rawTables.registro_actividad.map((r) => [r["Row ID"], r]),
  );

  // Define columns
  worksheet.columns = [
    { header: "Obra", key: "obra", width: 20 },
    { header: "Ciudad", key: "ciudad", width: 15 },
    { header: "Nombre", key: "nombre", width: 20 },
    { header: "Apellido", key: "apellido", width: 20 },
    { header: "Identificación", key: "identificacion", width: 15 },
    { header: "Estado", key: "estado", width: 15 },
    {
      header: "Horas Trabajadas Operador",
      key: "horas_trabajadas_operador",
      width: 20,
    },
    { header: "HEOD", key: "heod", width: 10 },
    { header: "HEON", key: "heon", width: 10 },
    { header: "HEFD", key: "hefd", width: 10 },
    { header: "HEFN", key: "hefn", width: 10 },
    { header: "RNO", key: "rno", width: 10 },
    { header: "RNF", key: "rnf", width: 10 },
    { header: "HF", key: "hf", width: 10 },
    { header: "Hora Inicial", key: "hora_inicial", width: 20 },
    { header: "Hora Final", key: "hora_final", width: 20 },
    { header: "Hora Inicio Descanso", key: "hora_inicio_descanso", width: 20 },
    { header: "Hora Fin Descanso", key: "hora_fin_descanso", width: 20 },
  ];

  // Style header row
  worksheet.getRow(1).font = { bold: true };
  worksheet.getRow(1).fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFD9D9D9" },
  };

  // Parse date strings to Excel date format (MM/DD/YYYY HH:MM:SS to Date object)
  // Use UTC to avoid timezone shifts (same approach as calculadora-extras)
  const parseDate = (dateStr) => {
    if (!dateStr) return "";
    try {
      // Format: "10/08/2025 07:30:00" or "3/2/26 7:00:00"
      const [datePart, timePart] = dateStr.split(" ");
      const [month, day, yearStr] = datePart.split("/").map(Number);
      const [hours, minutes, seconds] = timePart
        ? timePart.split(":").map(Number)
        : [0, 0, 0];

      // Handle 2-digit year
      let year = yearStr;
      if (year < 100) {
        year += 2000;
      }

      // Use Date.UTC to avoid timezone conversion
      return new Date(
        Date.UTC(year, month - 1, day, hours, minutes, seconds || 0),
      );
    } catch (e) {
      return dateStr; // Return original if parsing fails
    }
  };

  // Add data rows and collect them for sorting
  const rows = reportData.map((record) => {
    const registro = registroMap.get(record.id_registro);
    const obra = obraMap.get(record.id_obra) || {
      nombre: record.id_obra,
      ciudad: "",
    };
    const usuario = usuarioMap.get(record.id_operador) || {
      nombre: record.id_operador,
      apellido: "",
      identificacion: "",
    };

    return {
      obra: obra.nombre,
      ciudad: obra.ciudad,
      nombre: usuario.nombre,
      apellido: usuario.apellido,
      identificacion: usuario.identificacion,
      estado: registro?.estado || "",
      horas_trabajadas_operador: registro?.horas_trabajadas_operador || 0,
      heod: record.heod,
      heon: record.heon,
      hefd: record.hefd,
      hefn: record.hefn,
      rno: record.rno,
      rnf: record.rnf,
      hf: record.hf,
      hora_inicial: parseDate(registro?.hora_inicial),
      hora_final: parseDate(registro?.hora_final),
      hora_inicio_descanso: parseDate(registro?.hora_inicial_receso),
      hora_fin_descanso: parseDate(registro?.hora_final_receso),
    };
  });

  // Sort by apellido ascending, then by obra
  rows.sort((a, b) => {
    const apellidoCompare = a.apellido.localeCompare(b.apellido);
    if (apellidoCompare !== 0) return apellidoCompare;
    return a.obra.localeCompare(b.obra);
  });

  // Add sorted rows to worksheet
  rows.forEach((row) => worksheet.addRow(row));

  // Format date columns (only time columns, not HF which is column N)
  const dateColumns = ["O", "P", "Q", "R"]; // Hora Inicial, Hora Final, Hora Inicio Descanso, Hora Fin Descanso
  dateColumns.forEach((col) => {
    worksheet.getColumn(col).numFmt = "mm/dd/yyyy hh:mm:ss";
  });

  // Create sheets per ciudad with summed data by employee
  const ciudadGroups = {};
  rows.forEach((row) => {
    const ciudad = row.ciudad || "Sin Ciudad";
    if (!ciudadGroups[ciudad]) {
      ciudadGroups[ciudad] = {};
    }

    const employeeKey = `${row.identificacion}|${row.nombre}|${row.apellido}`;
    if (!ciudadGroups[ciudad][employeeKey]) {
      ciudadGroups[ciudad][employeeKey] = {
        nombre: row.nombre,
        apellido: row.apellido,
        identificacion: row.identificacion,
        heod: 0,
        heon: 0,
        hefd: 0,
        hefn: 0,
        rno: 0,
        rnf: 0,
        hf: 0,
      };
    }

    // Sum the values
    ciudadGroups[ciudad][employeeKey].heod += row.heod || 0;
    ciudadGroups[ciudad][employeeKey].heon += row.heon || 0;
    ciudadGroups[ciudad][employeeKey].hefd += row.hefd || 0;
    ciudadGroups[ciudad][employeeKey].hefn += row.hefn || 0;
    ciudadGroups[ciudad][employeeKey].rno += row.rno || 0;
    ciudadGroups[ciudad][employeeKey].rnf += row.rnf || 0;
    ciudadGroups[ciudad][employeeKey].hf += row.hf || 0;
  });

  // Create a sheet for each ciudad
  Object.keys(ciudadGroups)
    .sort()
    .forEach((ciudad) => {
      const ciudadWorksheet = workbook.addWorksheet(ciudad);

      // Define columns for ciudad sheet
      ciudadWorksheet.columns = [
        { header: "Nombre", key: "nombre", width: 20 },
        { header: "Apellido", key: "apellido", width: 20 },
        { header: "Identificación", key: "identificacion", width: 15 },
        { header: "HEOD", key: "heod", width: 10 },
        { header: "HEON", key: "heon", width: 10 },
        { header: "HEFD", key: "hefd", width: 10 },
        { header: "HEFN", key: "hefn", width: 10 },
        { header: "RNO", key: "rno", width: 10 },
        { header: "RNF", key: "rnf", width: 10 },
        { header: "HF", key: "hf", width: 10 },
      ];

      // Style header row
      ciudadWorksheet.getRow(1).font = { bold: true };
      ciudadWorksheet.getRow(1).fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FFD9D9D9" },
      };

      // Get employee data, sort by apellido, and add to worksheet
      const employeeRows = Object.values(ciudadGroups[ciudad]);
      employeeRows.sort((a, b) => a.apellido.localeCompare(b.apellido));
      employeeRows.forEach((empRow) => ciudadWorksheet.addRow(empRow));
    });

  // Generate buffer
  const buffer = await workbook.xlsx.writeBuffer();
  return buffer;
}

export async function generateBitakoraExcel(reportData, rawTables, rangeEnd) {
  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet("Bitakora");

  // Create maps for lookups
  const obraMap = new Map(
    rawTables.obra.map((o) => [
      o["Row ID"],
      { nombre: o.nombre_obra, ciudad: o.ciudad || "" },
    ]),
  );
  const usuarioMap = new Map(
    rawTables.usuario.map((u) => [
      u["Row ID"],
      {
        identificacion: u.identificacion || "",
      },
    ]),
  );

  // Define columns based on COLUMNAS_BITAKORA
  worksheet.columns = [
    { header: "Identificacion", key: "identificacion", width: 15 },
    { header: "Fecha", key: "fecha", width: 15 },
    { header: "IdItem", key: "idItem", width: 15 },
    { header: "Horas", key: "horas", width: 10 },
    { header: "HoraInicial", key: "horaInicial", width: 15 },
    { header: "IdCentroCosto", key: "idCentroCosto", width: 15 },
    { header: "IdFondo", key: "idFondo", width: 15 },
    { header: "PlanCuentaContable", key: "planCuentaContable", width: 20 },
    { header: "Observacion", key: "observacion", width: 30 },
    { header: "Descontar en prima", key: "descontarEnPrima", width: 15 },
    { header: "Item", key: "item", width: 30 },
    { header: "CentroCosto", key: "centroCosto", width: 15 },
    { header: "Fondo", key: "fondo", width: 15 },
  ];

  // Style header row
  worksheet.getRow(1).font = { bold: true };
  worksheet.getRow(1).fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFD9D9D9" },
  };

  // Format rangeEnd as dd/mm/yyyy
  const formatDate = (date) => {
    const day = String(date.getDate()).padStart(2, "0");
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const year = date.getFullYear();
    return `${day}/${month}/${year}`;
  };
  const fechaFormatted = formatDate(rangeEnd);

  // Group data by empleado, ciudad - sum hours for each concepto
  const employeeCiudadData = {};

  reportData.forEach((record) => {
    const usuario = usuarioMap.get(record.id_operador);
    const obra = obraMap.get(record.id_obra);
    const identificacion = usuario?.identificacion || "";
    const ciudad = obra?.ciudad || "";

    const key = `${identificacion}|${ciudad}`;

    if (!employeeCiudadData[key]) {
      employeeCiudadData[key] = {
        identificacion,
        ciudad,
        conceptos: {
          heod: 0,
          heon: 0,
          hefd: 0,
          hefn: 0,
          rno: 0,
          rnf: 0,
          hf: 0,
        },
      };
    }

    // Sum hours for each concepto
    employeeCiudadData[key].conceptos.heod += record.heod || 0;
    employeeCiudadData[key].conceptos.heon += record.heon || 0;
    employeeCiudadData[key].conceptos.hefd += record.hefd || 0;
    employeeCiudadData[key].conceptos.hefn += record.hefn || 0;
    employeeCiudadData[key].conceptos.rno += record.rno || 0;
    employeeCiudadData[key].conceptos.rnf += record.rnf || 0;
    employeeCiudadData[key].conceptos.hf += record.hf || 0;
  });

  // Generate rows for each empleado+ciudad combination and concepto
  const rows = [];
  Object.values(employeeCiudadData).forEach((employeeCiudad) => {
    // For each concepto type, create a row if hours > 0
    Object.entries(employeeCiudad.conceptos).forEach(([conceptoKey, horas]) => {
      if (horas > 0) {
        const conceptoInfo = CONCEPTOS_BITAKORA[conceptoKey];
        rows.push({
          identificacion: employeeCiudad.identificacion,
          fecha: fechaFormatted,
          idItem: conceptoInfo.idItem,
          horas: horas,
          horaInicial: "",
          idCentroCosto: "",
          idFondo: "",
          planCuentaContable: "",
          observacion: employeeCiudad.ciudad,
          descontarEnPrima: "",
          item: conceptoInfo.item,
          centroCosto: "",
          fondo: "",
        });
      }
    });
  });

  // Sort rows by identificacion, then by ciudad
  rows.sort((a, b) => {
    const idCompare = a.identificacion.localeCompare(b.identificacion);
    if (idCompare !== 0) return idCompare;
    return a.observacion.localeCompare(b.observacion);
  });

  // Add rows to worksheet
  rows.forEach((row) => worksheet.addRow(row));

  // Generate buffer
  const buffer = await workbook.xlsx.writeBuffer();
  return buffer;
}

export async function sendMonthlyReportEmail(
  transformedData,
  rawTables,
  transporter,
) {
  // Get cutoff date logic
  const now = new Date();
  const bogotaTime = new Date(
    now.toLocaleString("en-US", { timeZone: "America/Bogota" }),
  );
  const dayOfMonth = bogotaTime.getDate();
  const currentMonth = bogotaTime.getMonth(); // 0-indexed
  const currentYear = bogotaTime.getFullYear();
  const currentHour = bogotaTime.getHours();

  // Only run before 12 PM (morning run)
  if (currentHour >= 12) {
    console.log(
      `⏰ Not morning (current hour: ${currentHour}). Skipping quincenal report.`,
    );
    return;
  }

  // Determine rangeStart and rangeEnd based on cutoff day (local Bogota time)
  let rangeStart, rangeEnd;

  // Get rangeStart from env variable if provided, otherwise use null (no start cap)
  const extrasReportFrom = process.env.EXTRAS_REPORT_FROM;
  if (extrasReportFrom) {
    // Parse format: YYYY-MM-DD
    const [year, month, day] = extrasReportFrom.split("-").map(Number);
    rangeStart = new Date(year, month - 1, day, 0, 0, 0);
    console.log(`📅 Using EXTRAS_REPORT_FROM: ${rangeStart.toISOString()}`);
  } else {
    rangeStart = null; // No start date cap
    console.log(
      "📅 No EXTRAS_REPORT_FROM set - processing all unprocessed records",
    );
  }

  if (dayOfMonth === 25) {
    // 25th: Include records up to 15th (23:59:59) of current month
    rangeEnd = new Date(currentYear, currentMonth, 15, 23, 59, 59);
  } else if (dayOfMonth === 10) {
    // 10th: Include records up to last day (23:59:59) of previous month
    rangeEnd = new Date(currentYear, currentMonth, 0, 23, 59, 59);
  } else {
    console.log("Not a report day (10th or 25th). Skipping monthly report.");
    return;
  }

  console.log(
    `📊 Generating report for date range: ${rangeStart ? rangeStart.toISOString() : "all time"} to ${rangeEnd.toISOString()}`,
  );

  // Lookups O(1) en vez de .find() por cada fila de fact_produccion (~21k).
  const obraMap = new Map(rawTables.obra.map((o) => [o["Row ID"], o]));
  const registroMap = new Map(
    rawTables.registro_actividad.map((r) => [r["Row ID"], r]),
  );

  // Filter records
  const reportData = transformedData.fact_produccion
    .filter((record) => {
      // Filter out COSTA RICA
      const obraRecord = obraMap.get(record.id_obra);
      if (obraRecord?.nombre_obra === "COSTA RICA") return false;

      // Filter by date range (hora_inicial)
      const registroRecord = registroMap.get(record.id_registro);
      if (!registroRecord?.hora_inicial) return false;

      // Filter by procesado_rrhh - only include unprocessed records
      if (registroRecord.procesado_rrhh === "Y") return false;

      // Parse hora_inicial (format: "MM/DD/YYYY HH:MM:SS")
      const [datePart] = registroRecord.hora_inicial.split(" ");
      const [month, day, year] = datePart.split("/").map(Number);
      const horaInicial = new Date(year, month - 1, day);

      // Filter by fortnight date range
      // Only check rangeStart if it's defined
      if (rangeStart && horaInicial < rangeStart) return false;
      if (horaInicial > rangeEnd) return false;

      // Filter by estado TERMINADO only
      if (registroRecord.estado !== "TERMINADO") return false; // Skip not finished

      return true;
    })
    .map((record) => ({
      ...record,
    }));

  // Guard de idempotencia: si no hay registros sin procesar, no se envía nada.
  // Una entrega duplicada del scheduler re-extrae, ve todo procesado (el bulk Add
  // termina en segundos) y aquí retorna sin mandar un segundo correo.
  if (reportData.length === 0) {
    console.log(
      "ℹ️ No hay registros sin procesar en el rango. No se envía correo.",
    );
    return;
  }

  const excelBuffer = await generateMonthlyReportExcel(
    reportData,
    rawTables,
    transformedData.dim_obra,
    transformedData.dim_usuario,
  );

  const bitakoraBuffer = await generateBitakoraExcel(
    reportData,
    rawTables,
    rangeEnd,
  );

  const recipients = process.env.EXTRAS_EMAIL_TO?.split(",")
    .map((email) => email.trim())
    .join(", ");

  if (!recipients) {
    console.warn(
      "⚠️ EXTRAS_EMAIL_TO not configured. Skipping monthly report email.",
    );
    return;
  }

  const currentDate = getBogotaDateString("dd/MM/yyyy");

  await transporter.sendMail({
    from: `"${process.env.EMAIL_NAME}" <${process.env.EMAIL_FROM}>`,
    to: recipients,
    subject: `📊 Reporte Quincenal de Horas Extras - ${currentDate}`,
    html: `
      <div style="font-family: Arial, sans-serif; padding: 20px;">
        <h2>Reporte Quincenal de Horas Extras</h2>
        <p>Adjunto encontrará el reporte quincenal de horas extras correspondiente al día ${currentDate}.</p>
        <p>Se incluyen dos archivos:</p>
        <ul>
          <li><strong>Reporte Quincenal:</strong> Detalle completo de horas extras por empleado y obra</li>
          <li><strong>Bitakora:</strong> Formato para carga en sistema de nómina</li>
        </ul>
      </div>
    `,
    attachments: [
      {
        filename: `reporte-quincenal-${getBogotaDateString("yyyy-MM-dd")}.xlsx`,
        content: excelBuffer,
      },
      {
        filename: `bitakora-${getBogotaDateString("yyyy-MM-dd")}.xlsx`,
        content: bitakoraBuffer,
      },
    ],
  });

  console.log(`📧 Monthly report email sent to: ${recipients}`);

  // Dedupe: fact_produccion genera filas extra por accesorio con el mismo
  // id_registro. Sin esto marcaríamos (y contaríamos) el mismo registro 2 veces.
  const registroIds = [...new Set(reportData.map((record) => record.id_registro))];
  console.log(
    `📝 Marcando procesado_rrhh en ${registroIds.length} registros...`,
  );

  const { successful, failed } = await updateRegistrosProcesadoRRHH(registroIds);

  if (successful.length > 0) {
    console.log(
      `✅ Successfully marked ${successful.length} registros as procesado_rrhh`,
    );
  }

  if (failed.length > 0) {
    console.error(
      `❌ Failed to mark ${failed.length} registros: ${failed.join(", ")}`,
    );
    console.error(
      `⚠️ Email was sent but ${failed.length} registros were NOT marked as processed`,
    );
  }
}

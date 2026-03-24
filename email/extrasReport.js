import ExcelJS from "exceljs";
import { getBogotaDateString } from "./utils.js";

async function updateRegistrosProcesadoRRHH(registroIds) {
  const appId = process.env.APP_ID;
  const appKey = process.env.APP_KEY;

  if (!appId || !appKey) {
    console.warn(
      "⚠️ APP_ID and APP_KEY not configured. Skipping AppSheet updates.",
    );
    return;
  }

  const url = `https://www.appsheet.com/api/v2/apps/${appId}/tables/registro_actividad/Action`;

  // Update records in batches
  for (const registroId of registroIds) {
    try {
      await fetch(url, {
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
          Rows: [
            {
              "Row ID": registroId,
              procesado_rrhh: "Y",
            },
          ],
        }),
      });
      console.log(`✅ Updated procesado_rrhh for registro ${registroId}`);
    } catch (error) {
      console.error(
        `❌ Failed to update registro ${registroId}:`,
        error.message,
      );
    }
  }
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
  const obraMap = new Map(dimObra.map((o) => [o.id_obra, o.nombre_obra]));
  // Use raw usuario table to get nombre field
  const usuarioMap = new Map(
    rawTables.usuario.map((u) => [u["Row ID"], u.nombre || u.usuario]),
  );
  const registroMap = new Map(
    rawTables.registro_actividad.map((r) => [r["Row ID"], r]),
  );

  // Define columns
  worksheet.columns = [
    { header: "Obra", key: "obra", width: 20 },
    { header: "Operador", key: "operador", width: 20 },
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

  // Add data rows
  reportData.forEach((record) => {
    const registro = registroMap.get(record.id_registro);

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

    worksheet.addRow({
      obra: obraMap.get(record.id_obra) || record.id_obra,
      operador: usuarioMap.get(record.id_operador) || record.id_operador,
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
    });
  });

  // Format date columns (only time columns, not HF which is column K)
  const dateColumns = ["L", "M", "N", "O"]; // Hora Inicial, Hora Final, Hora Inicio Descanso, Hora Fin Descanso
  dateColumns.forEach((col) => {
    worksheet.getColumn(col).numFmt = "mm/dd/yyyy hh:mm:ss";
  });

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

  // Parse EXTRAS_REPORT_FROM (format: DD-MM-YYYY) - this is the absolute start date
  const reportFromStr = process.env.EXTRAS_REPORT_FROM;
  let rangeStart;
  if (reportFromStr) {
    const [day, month, year] = reportFromStr.split("-").map(Number);
    rangeStart = new Date(year, month - 1, day);
  }

  // Determine rangeEnd based on cutoff day
  let rangeEnd;

  if (dayOfMonth === 25) {
    // 25th: Include records up to 15th of current month
    rangeEnd = new Date(currentYear, currentMonth, 15, 23, 59, 59);
  } else if (dayOfMonth === 10) {
    // 10th: Include records up to last day of previous month
    rangeEnd = new Date(currentYear, currentMonth, 0, 23, 59, 59);
  } else {
    console.log("Not a report day (10th or 25th). Skipping monthly report.");
    return;
  }

  console.log(
    `📊 Generating report for date range: ${rangeStart ? rangeStart.toISOString() : "all time"} to ${rangeEnd.toISOString()}`,
  );

  // Filter records
  const reportData = transformedData.fact_produccion
    .filter((record) => {
      // Filter out COSTA RICA
      const obraRecord = rawTables.obra.find(
        (o) => o["Row ID"] === record.id_obra,
      );
      if (obraRecord?.nombre_obra === "COSTA RICA") return false;

      // Filter by date range (hora_inicial)
      const registroRecord = rawTables.registro_actividad.find(
        (r) => r["Row ID"] === record.id_registro,
      );
      if (!registroRecord?.hora_inicial) return false;

      // Parse hora_inicial (format: "MM/DD/YYYY HH:MM:SS")
      const [datePart] = registroRecord.hora_inicial.split(" ");
      const [month, day, year] = datePart.split("/").map(Number);
      const horaInicial = new Date(year, month - 1, day);

      // Check rangeStart only if EXTRAS_REPORT_FROM is set
      if (rangeStart && horaInicial < rangeStart) return false;
      if (horaInicial > rangeEnd) return false;

      // Filter by procesado_rrhh (not yet processed) AND estado TERMINADO
      if (registroRecord.procesado_rrhh === "Y") return false; // Skip already processed
      if (registroRecord.estado !== "TERMINADO") return false; // Skip not finished

      return true;
    })
    .map((record) => ({
      ...record,
    }));

  const excelBuffer = await generateMonthlyReportExcel(
    reportData,
    rawTables,
    transformedData.dim_obra,
    transformedData.dim_usuario,
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
        <p>El reporte incluye la siguiente información:</p>
        <ul>
          <li>Obra</li>
          <li>Operador</li>
          <li>Estado</li>
          <li>Horas Trabajadas Operador</li>
          <li>Horas Extra Ordinarias Diurnas (HEOD)</li>
          <li>Horas Extra Ordinarias Nocturnas (HEON)</li>
          <li>Horas Extra Festivas Diurnas (HEFD)</li>
          <li>Horas Extra Festivas Nocturnas (HEFN)</li>
          <li>Recargo Nocturno Ordinario (RNO)</li>
          <li>Recargo Nocturno Festivo (RNF)</li>
          <li>Horas Festivas (HF)</li>
          <li>Horarios de trabajo y descanso</li>
        </ul>
      </div>
    `,
    attachments: [
      {
        filename: `reporte-quincenal-${getBogotaDateString("yyyy-MM-dd")}.xlsx`,
        content: excelBuffer,
      },
    ],
  });

  console.log(`📧 Monthly report email sent to: ${recipients}`);

  // Update procesado_rrhh to Y for all processed records
  const registroIds = reportData.map((record) => record.id_registro);
  console.log(
    `📝 Updating procesado_rrhh for ${registroIds.length} records...`,
  );
  await updateRegistrosProcesadoRRHH(registroIds);
  console.log(`✅ All records updated in AppSheet`);
}

import { fileURLToPath } from "url";
import fs from "fs";
import handlebars from "handlebars";
import { getBogotaDateString } from "./utils.js";

// Register Handlebars helpers
handlebars.registerHelper("or", function (...args) {
  // Remove the options object (last argument)
  const options = args.pop();
  return args.some(Boolean);
});

export function renderTemplate(data) {
  const fileUrl = new URL("./plantilla-resumen-consumos.hbs", import.meta.url);
  const filePath = fileURLToPath(fileUrl);
  const templateString = fs.readFileSync(filePath, "utf-8");
  const compiled = handlebars.compile(templateString);
  return compiled(data);
}

export function renderMissingDataTemplate(data) {
  const fileUrl = new URL("./datos-faltantes.hbs", import.meta.url);
  const filePath = fileURLToPath(fileUrl);
  const templateString = fs.readFileSync(filePath, "utf-8");
  const compiled = handlebars.compile(templateString);
  return compiled(data);
}

export async function sendMissingDataEmail(missingDataTracker, transporter) {
  // Convert Map to array for template
  const obras = [];
  for (const [nombre, missing_data] of missingDataTracker) {
    // Convert viaje Map to array for template
    const viajesArray = [];
    if (missing_data.precioEquipo.viaje.size > 0) {
      for (const [destino, etiquetas] of missing_data.precioEquipo.viaje) {
        viajesArray.push({
          destino,
          etiquetas: etiquetas.join(", "),
        });
      }
    }

    // Only include obra if it has actual missing data
    const hasMissingData =
      missing_data.horario.missing ||
      missing_data.salario.length > 0 ||
      missing_data.precioEquipo.hora.length > 0 ||
      missing_data.precioEquipo.dia.length > 0 ||
      viajesArray.length > 0;

    if (hasMissingData) {
      obras.push({
        nombre,
        horario: missing_data.horario,
        salario: missing_data.salario,
        precioEquipo: {
          hora: missing_data.precioEquipo.hora,
          dia: missing_data.precioEquipo.dia,
          viaje: viajesArray,
        },
      });
    }
  }

  const html = renderMissingDataTemplate({
    date: getBogotaDateString("dd/MM/yyyy"),
    hasMissingData: obras.length > 0,
    obras,
  });

  const recipients = process.env.EMAIL_TO?.split(",")
    .map((email) => email.trim())
    .join(", ");

  if (!recipients) {
    console.warn("⚠️ EMAIL_TO not configured. Skipping email.");
    return;
  }

  await transporter.sendMail({
    from: `"${process.env.EMAIL_NAME}" <${process.env.EMAIL_FROM}>`,
    to: recipients,
    subject: `📊 Reporte de Datos Faltantes - ETL ${getBogotaDateString(
      "dd/MM/yyyy"
    )}`,
    html,
  });

  console.log(`📧 Email sent to: ${recipients}`);
}

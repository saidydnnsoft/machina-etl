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
import { calcularHorasTrabajadas } from "./calculadora-extras/utilitarios.js";
import { CONCEPTOS_EXTRAS } from "./calculadora-extras/conceptosExtras.js";
import { calcularHorasExtras } from "./calculadora-extras/calculadoraHorasExtras.js";
import { HORAS_MAXIMAS_ORDINARIAS } from "./calculadora-extras/constantes.js";

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

// --- Transformation ---
function formatDate(dateStr, includeTime = false) {
  if (!dateStr) return null;

  // Split into date and time parts (e.g., "10/08/2025 07:30:00")
  const parts = dateStr.split(" ");
  const datePart = parts[0];
  const timePart = parts[1] || "";

  // Parse date (MM/DD/YYYY format)
  const [month, day, year] = datePart.split("/");
  if (!year || !month || !day) return null;

  const formattedDate = `${year}-${month.padStart(2, "0")}-${day.padStart(
    2,
    "0"
  )}`;

  if (!includeTime || !timePart) {
    return formattedDate;
  }

  // Include timestamp for sorting
  return `${formattedDate}T${timePart}`;
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
  return equipos_arr.map((e) => {
    const etiqueta_equipo_record =
      etiquetas_equipos_map.get(e.id_etiqueta_equipo) || null;
    return {
      id_equipo: e["Row ID"],
      codigo_interno: e.codigo_interno ?? null,
      descripcion: e.descripcion ?? null,
      tipo_equipo: e.tipo_equipo ?? etiqueta_equipo_record?.tipo_equipo ?? null,
      tipo_activo: e.tipo_activo ?? etiqueta_equipo_record?.tipo_activo ?? null,
      etiqueta_equipo: etiqueta_equipo_record?.etiqueta ?? null,
    };
  });
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
      fecha_texto: `${day.toString().padStart(2, "0")}/${month
        .toString()
        .padStart(2, "0")}/${year}`,
      anio: year,
      num_mes: month,
      nombre_mes: `${month.toString().padStart(2, "0")}-${months[month - 1]}`,
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
    registro_actividad,
    equipo,
    usuario,
    obra,
    precio_etiqueta,
    etiquetas_equipos,
    viaje,
    horarios_obras,
    historico_salario,
    destino,
  } = rawData;

  const registro_actividad_map = new Map();
  registro_actividad.forEach((r) => {
    if (!registro_actividad_map.has(r.operador)) {
      registro_actividad_map.set(r.operador, new Map());
    }
    const fechas_map = registro_actividad_map.get(r.operador);
    if (!fechas_map.has(formatDate(r.hora_inicial))) {
      fechas_map.set(formatDate(r.hora_inicial), []);
    }
    fechas_map.get(formatDate(r.hora_inicial)).push(r);
    fechas_map
      .get(formatDate(r.hora_inicial))
      .sort(
        (a, b) =>
          new Date(a.hora_inicial).getTime() -
          new Date(b.hora_inicial).getTime()
      );
  });

  const horarios_obras_map = new Map();
  horarios_obras.forEach((h) => {
    if (!horarios_obras_map.has(h.id_obra)) {
      horarios_obras_map.set(h.id_obra, []);
    }
    horarios_obras_map.get(h.id_obra).push(h);
    horarios_obras_map
      .get(h.id_obra)
      .sort(
        (a, b) =>
          new Date(b.vigente_desde).getTime() -
          new Date(a.vigente_desde).getTime()
      );
  });

  const salarios_map = new Map();
  historico_salario.forEach((s) => {
    if (!salarios_map.has(s.id_usuario)) {
      salarios_map.set(s.id_usuario, []);
    }
    salarios_map.get(s.id_usuario).push(s);
  });

  const conceptos_extras = CONCEPTOS_EXTRAS;

  const equipos_map = new Map(equipo.map((e) => [e["Row ID"], e]));
  const etiquetas_equipos_map = new Map(
    etiquetas_equipos.map((e) => [e["Row ID"], e])
  );
  const viajes_map = new Map(viaje.map((v) => [v["Row ID"], v]));
  const obras_map = new Map(obra.map((o) => [o["Row ID"], o]));
  const usuarios_map = new Map(usuario.map((u) => [u["Row ID"], u]));
  const destinos_map = new Map(destino.map((d) => [d["Row ID"], d]));

  const precios_etiquetas_map = new Map();
  precio_etiqueta.forEach((p) => {
    const llave = `${p.id_obra}-${p.id_etiqueta_equipo}-${p.unidad_medida}`;

    if (p.unidad_medida === "VIAJE") {
      if (!precios_etiquetas_map.has(llave)) {
        precios_etiquetas_map.set(llave, new Map());
      }
      const destinos_map = precios_etiquetas_map.get(llave);

      if (!destinos_map.has(p.id_destino)) {
        destinos_map.set(p.id_destino, []);
      }
      destinos_map.get(p.id_destino).push(p);
    } else {
      if (!precios_etiquetas_map.has(llave)) {
        precios_etiquetas_map.set(llave, []);
      }
      precios_etiquetas_map.get(llave).push(p);
    }
  });
  const fact_produccion = [];
  for (const [_, fechas_map] of registro_actividad_map) {
    for (const [_, registros] of fechas_map) {
      const registros_para_calcular_extras = [];

      registros.forEach((r, index) => {
        const obra_record = obras_map.get(r.id_obra);
        if (r.hora_final && obra_record?.nombre_obra !== "COSTA RICA") {
          const horario_obra = horarios_obras_map
            .get(r.id_obra)
            ?.find(
              (h) =>
                new Date(h.vigente_desde).getTime() <=
                new Date(r.hora_inicial).getTime()
            );

          if (
            !horario_obra ||
            !horario_obra?.num_horas_lunes ||
            !horario_obra?.num_horas_martes ||
            !horario_obra?.num_horas_miercoles ||
            !horario_obra?.num_horas_jueves ||
            !horario_obra?.num_horas_viernes ||
            !horario_obra?.num_horas_sabado ||
            !horario_obra?.num_horas_festivas
          ) {
            console.log(
              `❌ Horario incompleto en la obra: ${obra_record.nombre_obra}`
            );
          }

          const horas_obligatorias_semana = horario_obra
            ? [
                0,
                parseFloat(horario_obra.num_horas_lunes || 8),
                parseFloat(horario_obra.num_horas_martes || 8),
                parseFloat(horario_obra.num_horas_miercoles || 8),
                parseFloat(horario_obra.num_horas_jueves || 8),
                parseFloat(horario_obra.num_horas_viernes || 8),
                parseFloat(horario_obra.num_horas_sabado || 4),
              ]
            : [0, 8, 8, 8, 8, 8, 4];
          const horas_max_festivas = horario_obra
            ? parseFloat(horario_obra.num_horas_festivas || 8)
            : 8;

          registros_para_calcular_extras.push({
            horaInicio: formatDate(r.hora_inicial, true),
            horaFin: formatDate(r.hora_final, true),
            horaInicioDescanso: formatDate(r.hora_inicial_receso, true),
            horaFinDescanso: formatDate(r.hora_final_receso, true),
            horasObligatoriasSemana: horas_obligatorias_semana,
            horasMaximasFestivas: horas_max_festivas,
            rowId: registros[0]["Row ID"],
          });
        }
      });

      const total_horas_trabajadas_dia = registros.reduce((acc, r) => {
        const obra_record = obras_map.get(r.id_obra);
        if (r.hora_final && obra_record?.nombre_obra !== "COSTA RICA") {
          const horas_trabajadas = calcularHorasTrabajadas(
            formatDate(r.hora_inicial, true),
            formatDate(r.hora_final, true),
            formatDate(r.hora_inicial_receso, true),
            formatDate(r.hora_final_receso, true)
          );
          return acc + horas_trabajadas;
        }
        return acc;
      }, 0);

      const extras = registros_para_calcular_extras.length
        ? calcularHorasExtras(registros_para_calcular_extras)
        : {
            heod: 0,
            heon: 0,
            hefd: 0,
            hefn: 0,
            rno: 0,
            rnf: 0,
            hf: 0,
          };

      // procesar registros e incluir las extras calculadas en el punto anterior en cada registro
      for (const registro of registros) {
        const operador_record = usuarios_map.get(registro.operador);
        const obra_record = obras_map.get(registro.id_obra);

        const horas_trabajadas_registro = registro.hora_final
          ? calcularHorasTrabajadas(
              formatDate(registro.hora_inicial, true),
              formatDate(registro.hora_final, true),
              formatDate(registro.hora_inicial_receso, true),
              formatDate(registro.hora_final_receso, true)
            )
          : 0;

        const salario_record = salarios_map
          .get(registro.operador)
          ?.find(
            (s) =>
              new Date(s.vigente_desde).getTime() <=
              new Date(registro.hora_inicial).getTime()
          );

        if (!salario_record) {
          console.log(
            `❌ No se encontró el salario del operador ${operador_record.nombre} en la obra ${obra_record.nombre_obra}`
          );
        }

        const porcentaje_a_cargar_de_extras =
          registro.hora_final && obra_record?.nombre_obra !== "COSTA RICA"
            ? horas_trabajadas_registro / total_horas_trabajadas_dia
            : 0;

        const extras_registro = {
          heod: parseFloat(
            (extras.heod * porcentaje_a_cargar_de_extras).toFixed(2)
          ),
          heon: parseFloat(
            (extras.heon * porcentaje_a_cargar_de_extras).toFixed(2)
          ),
          hefd: parseFloat(
            (extras.hefd * porcentaje_a_cargar_de_extras).toFixed(2)
          ),
          hefn: parseFloat(
            (extras.hefn * porcentaje_a_cargar_de_extras).toFixed(2)
          ),
          rno: parseFloat(
            (extras.rno * porcentaje_a_cargar_de_extras).toFixed(2)
          ),
          rnf: parseFloat(
            (extras.rnf * porcentaje_a_cargar_de_extras).toFixed(2)
          ),
          hf: parseFloat(
            (extras.hf * porcentaje_a_cargar_de_extras).toFixed(2)
          ),
        };

        const concepto_extra_aplicable = conceptos_extras.find(
          (c) =>
            new Date(c.vigente_desde).getTime() <=
            new Date(registro.hora_inicial).getTime()
        );

        const horas_maximas_ordinarias_mensuales =
          HORAS_MAXIMAS_ORDINARIAS.find(
            (h) =>
              new Date(h.vigente_desde).getTime() <=
              new Date(registro.hora_inicial).getTime()
          )?.horas_maximas_mensuales ?? 240;

        const valor_extras_y_recargos = Object.keys(extras_registro).reduce(
          (acc, key) =>
            (parseFloat(salario_record?.salario ?? 0) /
              horas_maximas_ordinarias_mensuales) *
              parseFloat(concepto_extra_aplicable[key] ?? 0) *
              parseFloat(extras_registro[key]) +
            acc,
          0
        );

        const equipo_record = equipos_map.get(registro.id_equipo);
        const etiqueta_equipo_record =
          etiquetas_equipos_map.get(equipo_record?.id_etiqueta_equipo) || null;
        const tipo_activo =
          equipo_record?.tipo_activo ??
          etiqueta_equipo_record?.tipo_activo ??
          null;
        const tipo_equipo =
          equipo_record?.tipo_equipo ??
          etiqueta_equipo_record?.tipo_equipo ??
          null;
        const related_viaje_ids =
          registro["Related viajes"]
            ?.split(",")
            .map((id) => id.trim())
            .filter((id) => id) ?? [];

        const info_precio_hora = precios_etiquetas_map.get(
          `${registro.id_obra}-${equipo_record?.id_etiqueta_equipo}-HORA`
        );

        const info_precio_dia = precios_etiquetas_map.get(
          `${registro.id_obra}-${equipo_record?.id_etiqueta_equipo}-DIA`
        );

        const info_precio_viaje = precios_etiquetas_map.get(
          `${registro.id_obra}-${equipo_record?.id_etiqueta_equipo}-VIAJE`
        );

        // HORA
        const info_precio_hora_por_fecha_descendiente = [
          ...(info_precio_hora || []),
        ].sort(
          (a, b) =>
            new Date(b.historico_desde).getTime() -
            new Date(a.historico_desde).getTime()
        );
        const precio_unitario_hora =
          info_precio_hora_por_fecha_descendiente?.find(
            (p) =>
              new Date(p.historico_desde).getTime() <=
              new Date(registro.hora_inicial).getTime()
          )?.precio || 0;

        if (!precio_unitario_hora && tipo_activo === "EQUIPO") {
          console.log(
            `❌ No se encontró el precio de la hora en la obra ${obra_record.nombre_obra} para la etiqueta ${etiqueta_equipo_record.etiqueta}`
          );
        }

        const valor_activo_por_horas =
          parseFloat(precio_unitario_hora) *
          parseFloat(registro.horas_trabajadas_maquina);

        // DIA
        const info_precio_dia_por_fecha_descendiente = [
          ...(info_precio_dia || []),
        ].sort(
          (a, b) =>
            new Date(b.historico_desde).getTime() -
            new Date(a.historico_desde).getTime()
        );
        const precio_unitario_dia =
          info_precio_dia_por_fecha_descendiente?.find(
            (p) =>
              new Date(p.historico_desde).getTime() <=
              new Date(registro.hora_inicial).getTime()
          )?.precio || 0;

        if (
          !precio_unitario_dia &&
          (tipo_equipo === "CAMIONETA" || tipo_activo === "MARTILLO")
        ) {
          console.log(
            `❌ No se encontró el precio del día en la obra ${obra_record.nombre_obra} para la etiqueta ${etiqueta_equipo_record.etiqueta}`
          );
        }

        if (
          !precio_unitario_dia &&
          (tipo_equipo === "CAMION" || tipo_activo === "VOLQUETA") &&
          !related_viaje_ids.length
        ) {
          console.log(
            `❌ No se encontró el precio del día en la obra ${obra_record.nombre_obra} para la etiqueta ${etiqueta_equipo_record.etiqueta}`
          );
        }

        const valor_activo_por_dia = parseFloat(precio_unitario_dia) * 1;

        // VIAJES
        const info_viajes = related_viaje_ids?.reduce(
          (acc, id) => {
            const record_viaje = viajes_map.get(id);
            if (!record_viaje) return acc;
            const destino = destinos_map?.get(
              record_viaje.id_destino
            )?.nombre_destino;

            const info_precio_destino =
              info_precio_viaje?.get(record_viaje.id_destino) || [];

            const info_precio_destino_por_fecha_descendiente = [
              ...info_precio_destino,
            ].sort(
              (a, b) =>
                new Date(b.historico_desde).getTime() -
                new Date(a.historico_desde).getTime()
            );

            const precio_unitario_viaje =
              info_precio_destino_por_fecha_descendiente?.find(
                (p) =>
                  new Date(p.historico_desde).getTime() <=
                  new Date(registro.hora_inicial).getTime()
              )?.precio || 0;

            if (
              !info_precio_dia &&
              !precio_unitario_viaje &&
              (tipo_equipo === "CAMION" || tipo_equipo === "VOLQUETA")
            ) {
              console.log(
                `❌ No se encontró el precio del viaje en la obra ${obra_record.nombre_obra} para la etiqueta ${etiqueta_equipo_record.etiqueta} al destino ${destino}`
              );
            }

            return {
              valor_viajes:
                acc.valor_viajes +
                parseFloat(precio_unitario_viaje) *
                  parseFloat(record_viaje.num_viajes),
              num_viajes: acc.num_viajes + parseFloat(record_viaje.num_viajes),
            };
          },
          { valor_viajes: 0, num_viajes: 0 }
        ) || { valor_viajes: 0, num_viajes: 0 };

        const valor_activo =
          valor_activo_por_horas +
          valor_activo_por_dia +
          info_viajes.valor_viajes;

        fact_produccion.push({
          id_registro: registro["Row ID"] ?? null,
          id_equipo: registro.id_equipo ?? null,
          id_operador: registro.operador ?? null,
          id_responsable_de_obra: registro.responsable_de_obra ?? null,
          id_obra: registro.id_obra ?? null,
          id_fecha: registro.hora_inicial
            ? formatDate(registro.hora_inicial)
            : null,
          estado: registro.estado ?? null,
          estado_aprobacion: registro.estado_aprobacion ?? null,
          varado: registro.varado === "Y" ? "Sí" : "No",
          horas_trabajadas_equipo: parseFloat(
            registro.horas_trabajadas_maquina || 0
          ),
          kilometros_recorridos: parseFloat(
            registro.kilometros_recorridos || 0
          ),
          horas_trabajadas_operador: parseFloat(
            horas_trabajadas_registro.toFixed(2)
          ),
          combustible:
            registro.unidad_de_medida === "Galones"
              ? parseFloat(registro.combustible || 0)
              : // se asume que si no es galon es litro
                parseFloat(
                  (parseFloat(registro.combustible || 0) * 0.264172).toFixed(2)
                ),
          horas_varado: parseFloat(registro.horas_varado || 0),
          heod: extras_registro.heod,
          heon: extras_registro.heon,
          hefd: extras_registro.hefd,
          hefn: extras_registro.hefn,
          rno: extras_registro.rno,
          rnf: extras_registro.rnf,
          hf: extras_registro.hf,
          valor_extras_y_recargos: parseFloat(
            (valor_extras_y_recargos || 0).toFixed(2)
          ),
          valor_activo: valor_activo,
          num_viajes: info_viajes.num_viajes,
        });
      }
    }
  }

  // console.log(
  //   "🚀 ~ transform_fact_produccion ~ fact_produccion:",
  //   fact_produccion
  // );

  return fact_produccion;
}

function transform(tables) {
  const transformed_data = {};

  // --- 1. Prepare Date Dimension ---
  const uniqueDateStrings = new Set();
  if (tables.registro_actividad) {
    tables.registro_actividad.forEach((r) => {
      if (r.hora_inicial) {
        const formattedDate = formatDate(r.hora_inicial);
        if (formattedDate) uniqueDateStrings.add(formattedDate);
      }
      if (r.hora_inicial) {
        const formattedDate = formatDate(r.hora_inicial);
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

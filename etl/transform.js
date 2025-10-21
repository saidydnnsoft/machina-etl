import {
  startOfMonth,
  startOfQuarter,
  startOfWeek,
  startOfYear,
} from "date-fns";
import { calcularHorasTrabajadas } from "../calculadora-extras/utilitarios.js";
import { CONCEPTOS_EXTRAS } from "../calculadora-extras/conceptosExtras.js";
import { calcularHorasExtras } from "../calculadora-extras/calculadoraHorasExtras.js";
import { HORAS_MAXIMAS_ORDINARIAS } from "../calculadora-extras/constantes.js";

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

  function updateMissingDataTracker(
    obra_name,
    category,
    subcategory,
    item_data
  ) {
    if (!missing_data_tracker.has(obra_name)) {
      missing_data_tracker.set(obra_name, {
        horario: { missing: false },
        salario: [],
        precioEquipo: {
          hora: [],
          viaje: new Map(),
          dia: [],
        },
      });
    }

    const obra_data = missing_data_tracker.get(obra_name);

    if (category === "horario") {
      obra_data.horario.missing = true;
    } else if (category === "salario") {
      if (!obra_data.salario.includes(item_data)) {
        obra_data.salario.push(item_data);
      }
    } else if (category === "precioEquipo") {
      if (subcategory === "hora") {
        if (!obra_data.precioEquipo.hora.includes(item_data)) {
          obra_data.precioEquipo.hora.push(item_data);
        }
      } else if (subcategory === "viaje") {
        const { destino, etiqueta } = item_data;
        if (!obra_data.precioEquipo.viaje.has(destino)) {
          obra_data.precioEquipo.viaje.set(destino, []);
        }
        if (!obra_data.precioEquipo.viaje.get(destino).includes(etiqueta)) {
          obra_data.precioEquipo.viaje.get(destino).push(etiqueta);
        }
      } else if (subcategory === "dia") {
        if (!obra_data.precioEquipo.dia.includes(item_data)) {
          obra_data.precioEquipo.dia.push(item_data);
        }
      }
    }
  }

  const missing_data_tracker = new Map();

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
            updateMissingDataTracker(obra_record.nombre_obra, "horario");
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
          updateMissingDataTracker(
            obra_record.nombre_obra,
            "salario",
            null,
            operador_record.usuario
          );
        }

        const porcentaje_registro_en_dia = registro.hora_final
          ? horas_trabajadas_registro / total_horas_trabajadas_dia
          : 0;

        const extras_registro = {
          heod: parseFloat(
            (extras.heod * porcentaje_registro_en_dia).toFixed(2)
          ),
          heon: parseFloat(
            (extras.heon * porcentaje_registro_en_dia).toFixed(2)
          ),
          hefd: parseFloat(
            (extras.hefd * porcentaje_registro_en_dia).toFixed(2)
          ),
          hefn: parseFloat(
            (extras.hefn * porcentaje_registro_en_dia).toFixed(2)
          ),
          rno: parseFloat((extras.rno * porcentaje_registro_en_dia).toFixed(2)),
          rnf: parseFloat((extras.rnf * porcentaje_registro_en_dia).toFixed(2)),
          hf: parseFloat((extras.hf * porcentaje_registro_en_dia).toFixed(2)),
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
          updateMissingDataTracker(
            obra_record.nombre_obra,
            "precioEquipo",
            "hora",
            etiqueta_equipo_record.etiqueta
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
          updateMissingDataTracker(
            obra_record.nombre_obra,
            "precioEquipo",
            "dia",
            etiqueta_equipo_record.etiqueta
          );
        }

        if (
          !precio_unitario_dia &&
          !related_viaje_ids.length &&
          (tipo_equipo === "CAMION" || tipo_activo === "VOLQUETA")
        ) {
          updateMissingDataTracker(
            obra_record.nombre_obra,
            "precioEquipo",
            "dia",
            etiqueta_equipo_record.etiqueta
          );
        }

        const horas_varado = parseFloat(registro.horas_varado || 0);
        const porcentaje_no_varado_equipo_registro_para_cobros_por_dia =
          (horas_trabajadas_registro - horas_varado) /
          horas_trabajadas_registro;

        const valor_activo_por_dia = parseFloat(
          precio_unitario_dia *
            porcentaje_registro_en_dia *
            porcentaje_no_varado_equipo_registro_para_cobros_por_dia
        );

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

            const precio_unitario_viaje = precio_unitario_dia
              ? 0
              : info_precio_destino_por_fecha_descendiente?.find(
                  (p) =>
                    new Date(p.historico_desde).getTime() <=
                    new Date(registro.hora_inicial).getTime()
                )?.precio || 0;

            if (
              !precio_unitario_dia &&
              !precio_unitario_viaje &&
              (tipo_equipo === "CAMION" || tipo_equipo === "VOLQUETA")
            ) {
              updateMissingDataTracker(
                obra_record.nombre_obra,
                "precioEquipo",
                "viaje",
                { destino, etiqueta: etiqueta_equipo_record.etiqueta }
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
          horas_varado,
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

  // // Log missing data summary
  // if (missing_data_tracker.size > 0) {
  //   console.log("📊 Datos faltantes:");
  //   for (const [obra_name, missing_data] of missing_data_tracker) {
  //     console.log(`\n🏗️ Obra: ${obra_name}`);

  //     if (missing_data.horario.missing) {
  //       console.log("  ⏰ Horario: Datos faltantes");
  //     }

  //     if (missing_data.salario.length > 0) {
  //       console.log(
  //         `  💰 Salario: Operadores: ${missing_data.salario.join(", ")}`
  //       );
  //     }

  //     const precio = missing_data.precioEquipo;
  //     if (
  //       precio.hora.length > 0 ||
  //       precio.dia.length > 0 ||
  //       precio.viaje.size > 0
  //     ) {
  //       console.log("  💲 Precio Equipo:");

  //       if (precio.hora.length > 0) {
  //         console.log(
  //           `    🕐 Hora: Etiquetas de equipos: ${precio.hora.join(", ")}`
  //         );
  //       }

  //       if (precio.dia.length > 0) {
  //         console.log(
  //           `    📅 Día: Etiquetas de equipos: ${precio.dia.join(", ")}`
  //         );
  //       }

  //       if (precio.viaje.size > 0) {
  //         console.log("    🚛 Viaje:");
  //         for (const [destino, etiquetas] of precio.viaje) {
  //           console.log(`      → ${destino}: ${etiquetas.join(", ")}`);
  //         }
  //       }
  //     }
  //   }
  // } else {
  //   console.log("✅ No missing data found!");
  // }

  return { fact_produccion, missing_data_tracker };
}

export function transform(tables) {
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
  const { fact_produccion, missing_data_tracker } =
    transform_fact_produccion(tables);
  transformed_data.fact_produccion = fact_produccion;
  transformed_data.missing_data_tracker = missing_data_tracker;

  return transformed_data;
}

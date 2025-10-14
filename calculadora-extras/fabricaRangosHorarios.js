import { Fecha } from "./fecha.js";
import { convertirHorasDecimalesATiempo } from "./utilitarios.js";
import { construirFechaUtc } from "./utilitarios.js";

export class FabricaRangosHorarios {
  constructor() {
    this.tiposRango = [
      { tipo: "Diurno", inicio: 6, fin: 19, vigente_desde: "2025-12-25" },
      { tipo: "Nocturno", inicio: 19, fin: 24, vigente_desde: "2025-12-25" },
      { tipo: "Diurno", inicio: 6, fin: 21, vigente_desde: "1970-01-01" },
      { tipo: "Nocturno", inicio: 21, fin: 24, vigente_desde: "1970-01-01" },
      { tipo: "Nocturno", inicio: 0, fin: 6, vigente_desde: "1970-01-01" },
    ];
  }

  construirRangosHorarios(
    fechaInicio,
    fechaFin,
    fechaInicioDescanso,
    fechaFinDescanso,
    fechasFestivas
  ) {
    const rangos = [];
    const huboDescanso = fechaInicioDescanso && fechaFinDescanso;

    if (huboDescanso) {
      this.procesarRango(
        fechaInicio,
        fechaInicioDescanso,
        rangos,
        fechasFestivas
      );
      this.procesarRango(fechaFinDescanso, fechaFin, rangos, fechasFestivas);
    }
    if (!huboDescanso)
      this.procesarRango(fechaInicio, fechaFin, rangos, fechasFestivas);

    return rangos;
  }

  procesarRango(inicio, fin, rangos, fechasFestivas) {
    let actual = inicio;

    while (actual.objectoFecha < new Date(fin.objectoFecha.getTime() - 60000)) {
      const inicioRangoActual = actual;
      const rangoAplicable = this.obtenerTipoDeRango(
        actual.obtenerHorasTotales,
        inicioRangoActual.objectoFecha
      );
      const menorEntreFinRangoAplicableYFin = actual.esMismoDia(fin)
        ? Math.min(rangoAplicable.fin, fin.obtenerHorasTotales)
        : rangoAplicable.fin;

      const diferenciaHorasProximoFinRango =
        menorEntreFinRangoAplicableYFin - actual.obtenerHorasTotales;

      const { horas, minutos, segundos } = convertirHorasDecimalesATiempo(
        diferenciaHorasProximoFinRango
      );

      const finRangoActual = new Fecha(
        construirFechaUtc(
          actual.anio,
          actual.mes,
          actual.dia,
          actual.horas + horas,
          actual.minutos + minutos,
          actual.segundos + segundos
        )
      );

      rangos.push({
        inicio: inicioRangoActual.obtenerHorasTotales,
        fin: finRangoActual.obtenerHorasTotales,
        rango: rangoAplicable.tipo,
        horas: diferenciaHorasProximoFinRango,
        diaSemana: inicioRangoActual.diaSemana,
        dia: inicioRangoActual.dia,
        mes: inicioRangoActual.mes,
        anio: inicioRangoActual.anio,
        indiceDiaSemana: inicioRangoActual.indiceDiaSemana,
        esFestivo: inicioRangoActual.esFestivo(fechasFestivas),
      });

      actual = finRangoActual;
    }
  }

  obtenerTipoDeRango(horas, fechaInicioRegistro) {
    for (const rango of this.tiposRango) {
      if (
        horas >= rango.inicio &&
        horas < rango.fin &&
        new Date(rango.vigente_desde).getTime() <=
          new Date(fechaInicioRegistro).getTime()
      )
        return rango;
    }
  }
}

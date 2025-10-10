import { FESTIVOS } from "./festivos.js";
import { calcularHorasTrabajadas, construirFechas } from "./utilitarios.js";
import {
  EspecificacionHoraInicioYFinRequeridas,
  EspecificacionAmbosDescansosRequeridos,
  EspecificacionInicioDescansoAnteriorAlFinDescanso,
  EspecificacionDescansoPosteriorAlInicio,
  EspecificacionDescansoAnteriorAlFin,
  EspecificacionInicioAntesDeFin,
  EspecificacionHorasObligatoriasSemanaValidas,
  EspecificacionMaximo24Horas,
  EspecificacionRangosNoSuperpuestos,
} from "./especificaciones.js";
import { FabricaRangosHorarios } from "./fabricaRangosHorarios.js";

export function calcularHorasExtras(registros) {
  const especificacionRangosNoSuperpuestos =
    new EspecificacionRangosNoSuperpuestos();
  if (!especificacionRangosNoSuperpuestos.cumpleCon(registros)) {
    return {
      heod: 0,
      heon: 0,
      hefd: 0,
      hefn: 0,
      rno: 0,
      rnf: 0,
      hf: 0,
    };
    throw new Error(especificacionRangosNoSuperpuestos.obtenerMensajeError());
  }

  const fechasFestivas = construirFechas(...FESTIVOS);
  const fabricaRangosHorarios = new FabricaRangosHorarios();
  const rangosHorarios = [];
  const registrosAumentados = [];
  for (const registro of registros.sort((a, b) => a.horaInicio - b.horaFin)) {
    const {
      horaInicio,
      horaFin,
      horasObligatoriasSemana,
      horasMaximasFestivas,
      horaInicioDescanso,
      horaFinDescanso,
      horasTrabajadas,
    } = registro;

    const [fechaInicio, fechaFin, fechaInicioDescanso, fechaFinDescanso] =
      construirFechas(horaInicio, horaFin, horaInicioDescanso, horaFinDescanso);

    registrosAumentados.push({
      ...registro,
      horasTrabajadas: calcularHorasTrabajadas(
        horaInicio,
        horaFin,
        horaInicioDescanso,
        horaFinDescanso
      ),
      fechaInicio,
      fechaFin,
      fechaInicioDescanso,
      fechaFinDescanso,
    });

    const especificacionHoraInicioYFin =
      new EspecificacionHoraInicioYFinRequeridas();
    const especificacionAmbosDescansos =
      new EspecificacionAmbosDescansosRequeridos();
    const especificacionInicioDescansoAnteriorAlFinDescanso =
      new EspecificacionInicioDescansoAnteriorAlFinDescanso();
    const especificacionDescansoPosteriorAlInicio =
      new EspecificacionDescansoPosteriorAlInicio();
    const especificacionDescansoAnteriorAlFin =
      new EspecificacionDescansoAnteriorAlFin();
    const especificacionInicioAntesDeFin = new EspecificacionInicioAntesDeFin();
    // const especificacionHorasObligatoriasSemana =
    //   new EspecificacionHorasObligatoriasSemanaValidas();
    // const especificacionMaximo24Horas = new EspecificacionMaximo24Horas();

    const huboDescanso = horaInicioDescanso && horaFinDescanso;

    const validaciones = [
      {
        spec: especificacionHoraInicioYFin,
        params: [fechaInicio, fechaFin],
      },
      {
        spec: especificacionAmbosDescansos,
        params: [fechaInicioDescanso, fechaFinDescanso],
      },
      {
        spec: especificacionInicioDescansoAnteriorAlFinDescanso,
        params: [
          huboDescanso,
          fechaInicioDescanso?.objectoFecha,
          fechaFinDescanso?.objectoFecha,
        ],
      },
      {
        spec: especificacionDescansoPosteriorAlInicio,
        params: [
          huboDescanso,
          fechaInicio?.objectoFecha,
          fechaInicioDescanso?.objectoFecha,
        ],
      },
      {
        spec: especificacionDescansoAnteriorAlFin,
        params: [
          huboDescanso,
          fechaFin?.objectoFecha,
          fechaFinDescanso?.objectoFecha,
        ],
      },
      {
        spec: especificacionInicioAntesDeFin,
        params: [fechaInicio?.objectoFecha, fechaFin?.objectoFecha],
      },
      // {
      //   spec: especificacionHorasObligatoriasSemana,
      //   params: [horasObligatoriasSemana],
      // },
      // {
      //   spec: especificacionMaximo24Horas,
      //   params: [fechaInicio?.obtenerDuracionEnHoras(fechaFin)],
      // },
    ];

    for (const { spec, params } of validaciones) {
      if (!spec.cumpleCon(...params)) {
        throw new Error(spec.obtenerMensajeError());
      }
    }

    const rangosHorariosRegistro =
      fabricaRangosHorarios.construirRangosHorarios(
        fechaInicio,
        fechaFin,
        fechaInicioDescanso,
        fechaFinDescanso,
        fechasFestivas
      );
    rangosHorarios.push(...rangosHorariosRegistro);
  }

  const extras = {
    heod: 0,
    heon: 0,
    hefd: 0,
    hefn: 0,
    rno: 0,
    rnf: 0,
    hf: 0,
  };

  const inicioEsFestivo =
    registrosAumentados[0].fechaInicio.esFestivo(fechasFestivas);
  const indiceSemanaInicio = registrosAumentados[0].fechaInicio.indiceDiaSemana;

  // La regla la pone la obra en donde se trabajó mas horas. En caso de empate, se toma el último registro (registro en el que terminó la jornada)
  const maxHorasTrabajadas = Math.max(
    ...registrosAumentados.map((r) => r.horasTrabajadas)
  );
  const indiceRegistroConMasHorasTrabajadas = registrosAumentados.findLastIndex(
    (registro) => registro.horasTrabajadas === maxHorasTrabajadas
  );
  const horasObligatoriasDia =
    registrosAumentados[indiceRegistroConMasHorasTrabajadas]
      .horasObligatoriasSemana[indiceSemanaInicio];
  const horasMaximasFestivas =
    registrosAumentados[indiceRegistroConMasHorasTrabajadas]
      .horasMaximasFestivas;

  let horasTrabajadasAcumuladas = 0;

  const horasMaximas = inicioEsFestivo
    ? horasMaximasFestivas
    : horasObligatoriasDia;

  rangosHorarios.forEach((rango) => {
    horasTrabajadasAcumuladas += rango.horas;

    const horasExtrasPorProcesarRango =
      horasTrabajadasAcumuladas <= horasMaximas
        ? 0
        : Math.min(horasTrabajadasAcumuladas - horasMaximas, rango.horas);
    const recargosPorProcesarRango = rango.horas - horasExtrasPorProcesarRango;

    // Horas festivas
    if (rango.esFestivo && inicioEsFestivo) {
      extras.hf += recargosPorProcesarRango;
    }

    // Recargos nocturnos
    if (
      rango.rango === "Nocturno" &&
      (!rango.esFestivo || (rango.esFestivo && inicioEsFestivo))
    ) {
      extras.rno += recargosPorProcesarRango;
    }

    // Recargos nocturnos festivos
    if (rango.rango === "Nocturno" && rango.esFestivo && !inicioEsFestivo) {
      extras.rnf += recargosPorProcesarRango;
    }

    // Horas extra diurnas
    if (rango.rango === "Diurno" && !rango.esFestivo) {
      extras.heod += horasExtrasPorProcesarRango;
    }

    // Horas extras nocturnas
    if (rango.rango === "Nocturno" && !rango.esFestivo) {
      extras.heon += horasExtrasPorProcesarRango;
    }

    // Horas extra festivas diurnas
    if (rango.rango === "Diurno" && rango.esFestivo) {
      extras.hefd += horasExtrasPorProcesarRango;
    }

    // Horas extra festivas nocturnas
    if (rango.rango === "Nocturno" && rango.esFestivo) {
      extras.hefn += horasExtrasPorProcesarRango;
    }
  });
  return extras;
}

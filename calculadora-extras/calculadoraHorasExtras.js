import { FESTIVOS } from "./festivos";
import { construirFechas } from "./utilitarios";
import {
  EspecificacionHoraInicioYFinRequeridas,
  EspecificacionAmbosDescansosRequeridos,
  EspecificacionInicioDescansoAnteriorAlFinDescanso,
  EspecificacionDescansoPosteriorAlInicio,
  EspecificacionDescansoAnteriorAlFin,
  EspecificacionInicioAntesDeFin,
  EspecificacionHorasObligatoriasSemanaValidas,
  EspecificacionMaximo24Horas,
} from "./especificaciones";
import { FabricaRangosHorarios } from "./fabricaRangosHorarios";

export function calcularHorasExtras(
  horaInicio,
  horaFin,
  horasObligatoriasSemana,
  horasMaximasFestivas,
  horaInicioDescansoInput = undefined,
  horaFinDescansoInput = undefined,
  records = undefined
) {
  const horaInicioDescanso =
    horaInicioDescansoInput === null ? undefined : horaInicioDescansoInput;
  const horaFinDescanso =
    horaFinDescansoInput === null ? undefined : horaFinDescansoInput;

  console.log(records);

  console.log("horaInicio", horaInicio);
  console.log("horaFin", horaFin);

  const fechasFestivas = construirFechas(...FESTIVOS);

  const [fechaInicio, fechaFin, fechaInicioDescanso, fechaFinDescanso] =
    construirFechas(horaInicio, horaFin, horaInicioDescanso, horaFinDescanso);

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
  const especificacionHorasObligatoriasSemana =
    new EspecificacionHorasObligatoriasSemanaValidas();
  const especificacionMaximo24Horas = new EspecificacionMaximo24Horas();

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
    {
      spec: especificacionHorasObligatoriasSemana,
      params: [horasObligatoriasSemana],
    },
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

  const fabricaRangosHorarios = new FabricaRangosHorarios();
  const rangosHorarios = fabricaRangosHorarios.construirRangosHorarios(
    fechaInicio,
    fechaFin,
    fechaInicioDescanso,
    fechaFinDescanso,
    fechasFestivas
  );

  const extras = {
    heod: 0,
    heon: 0,
    hefd: 0,
    hefn: 0,
    rno: 0,
    rnf: 0,
    hf: 0,
  };

  // console.log(rangosHorarios);

  const inicioEsFestivo = fechaInicio.esFestivo(fechasFestivas);
  const indiceSemanaInicio = fechaInicio.indiceDiaSemana;
  const horasObligatoriasDia = horasObligatoriasSemana[indiceSemanaInicio];
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

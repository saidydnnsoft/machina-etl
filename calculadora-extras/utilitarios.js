import { EspecificacionFechaValida } from "./especificaciones.js";
import { Fecha } from "./fecha.js";

export function convertirHorasDecimalesATiempo(horasDecimales) {
  const horas = Math.floor(horasDecimales);
  const minutosDesdeDecimal = (horasDecimales - horas) * 60;
  const minutos = Math.floor(minutosDesdeDecimal);
  const segundosDesdeDecimal = (minutosDesdeDecimal - minutos) * 60;
  const segundos = Math.round(segundosDesdeDecimal);

  return {
    horas,
    minutos,
    segundos,
  };
}

export function construirFechaAPartirDeTexto(textoFecha) {
  const especficacionFechaValida = new EspecificacionFechaValida();
  if (!especficacionFechaValida.cumpleCon(textoFecha)) {
    throw new Error(especficacionFechaValida.obtenerMensajeError());
  }

  const tieneHora = textoFecha.includes("T");

  const [parteFecha, parteHora] = textoFecha.split("T");
  const [anio, mes, dia] = parteFecha.split("-").map(Number);

  const [horas, minutos] = tieneHora
    ? parteHora.split(":").map(Number)
    : [0, 0];

  return construirFechaUtc(anio, mes, dia, horas, minutos);
}

export function construirFechaUtc(anio, mes, dia, horas, minutos) {
  return new Date(Date.UTC(anio, mes - 1, dia, horas, minutos));
}

export function construirFechas(...textoFechas) {
  return textoFechas.map((textoFecha) => {
    if (!textoFecha) return undefined;
    return new Fecha(construirFechaAPartirDeTexto(textoFecha));
  });
}

export function calcularHorasTrabajadas(
  horaInicio,
  horaFin,
  horaInicioDescanso,
  horaFinDescanso
) {
  const duracionDescanso =
    new Date(horaFinDescanso) - new Date(horaInicioDescanso) || 0;

  return (
    (new Date(horaFin) - new Date(horaInicio) - duracionDescanso) /
    1000 /
    60 /
    60
  );
}

export function validarRangosNoSuperpuestos(dateRanges) {
  const parsedDates = dateRanges.map((range) => {
    const start = construirFechaAPartirDeTexto(range.start);
    const end = construirFechaAPartirDeTexto(range.end);

    if (end <= start) {
      throw new Error(
        `Invalid date range: The end date must be after the start date. Start: ${range.start}, End: ${range.end}`
      );
    }

    return { start, end };
  });

  parsedDates.sort((a, b) => a.start.getTime() - b.start.getTime());

  for (let i = 0; i < parsedDates.length - 1; i++) {
    if (parsedDates[i + 1].start < parsedDates[i].end) {
      return false;
    }
  }

  return true;
}

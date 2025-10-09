import { EspecificacionFechaValida } from "./especificaciones";
import { Fecha } from "./fecha";

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

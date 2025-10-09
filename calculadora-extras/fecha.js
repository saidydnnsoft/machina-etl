export class Fecha {
  constructor(fechaObj) {
    this.fecha = fechaObj;
  }

  get anio() {
    return this.fecha.getUTCFullYear();
  }

  get mes() {
    return this.fecha.getUTCMonth() + 1;
  }

  get dia() {
    return this.fecha.getUTCDate();
  }

  get horas() {
    return this.fecha.getUTCHours();
  }

  get minutos() {
    return this.fecha.getUTCMinutes();
  }

  get segundos() {
    return this.fecha.getUTCSeconds();
  }

  get diaSemana() {
    const diasSemana = [
      "Domingo",
      "Lunes",
      "Martes",
      "Miercoles",
      "Jueves",
      "Viernes",
      "Sabado",
    ];
    return diasSemana[this.indiceDiaSemana];
  }

  get indiceDiaSemana() {
    return this.fecha.getUTCDay();
  }

  get isoString() {
    return this.fecha.toISOString();
  }

  get objectoFecha() {
    return this.fecha;
  }

  get obtenerHorasTotales() {
    const totalHoras = this.horas + this.minutos / 60 + this.segundos / 3600;
    return Math.round(totalHoras * 100) / 100;
  }

  obtenerDuracionEnHoras(otraFecha) {
    const duration =
      Math.abs(this.objectoFecha - otraFecha.objectoFecha) / 1000 / 60 / 60;
    return duration;
  }

  esMismoDia(otraFecha) {
    return (
      this.dia === otraFecha.dia &&
      this.mes === otraFecha.mes &&
      this.anio === otraFecha.anio
    );
  }

  esFestivo(festivos) {
    return (
      this.diaSemana === "Domingo" ||
      festivos.some((festivo) => festivo.esMismoDia(this))
    );
  }

  esOtroDia(otraFecha) {
    return !this.esMismoDia(otraFecha);
  }
}

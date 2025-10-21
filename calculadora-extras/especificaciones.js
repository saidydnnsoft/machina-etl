import { validarRangosNoSuperpuestos } from "./utilitarios.js";
import { HORAS_MAXIMAS_ORDINARIAS } from "./constantes.js";

export class EspecificacionHoraInicioYFinRequeridas {
  cumpleCon(horaInicio, horaFin) {
    return horaInicio && horaFin;
  }
  obtenerMensajeError() {
    return "Se deben incluir ambos horarios de inicio y fin";
  }
}

export class EspecificacionAmbosDescansosRequeridos {
  cumpleCon(horaInicioDescanso, horaFinDescanso) {
    return (
      (!horaInicioDescanso && !horaFinDescanso) ||
      (horaInicioDescanso && horaFinDescanso)
    );
  }
  obtenerMensajeError() {
    return "Si hubo descanso se deben incluir ambos horarios";
  }
}

export class EspecificacionInicioDescansoAnteriorAlFinDescanso {
  cumpleCon(huboDescanso, horaInicioDescanso, horaFinDescanso) {
    return huboDescanso ? horaInicioDescanso < horaFinDescanso : true;
  }

  obtenerMensajeError() {
    return "La hora de inicio del descanso debe ser anterior al fin del descanso";
  }
}

export class EspecificacionDescansoPosteriorAlInicio {
  cumpleCon(huboDescanso, horaInicio, horaInicioDescanso) {
    return huboDescanso ? horaInicioDescanso > horaInicio : true;
  }
  obtenerMensajeError() {
    return "La hora de inicio del descanso debe ser posterior al inicio del turno";
  }
}

export class EspecificacionDescansoAnteriorAlFin {
  cumpleCon(huboDescanso, horaFin, horaFinDescanso) {
    return huboDescanso ? horaFinDescanso < horaFin : true;
  }
  obtenerMensajeError() {
    return "La hora de fin del descanso debe ser anterior al fin del turno";
  }
}

export class EspecificacionInicioAntesDeFin {
  cumpleCon(horaInicio, horaFin) {
    return horaInicio < horaFin;
  }
  obtenerMensajeError() {
    return "La hora de inicio debe ser anterior a la hora de fin";
  }
}

export class EspecificacionHorasObligatoriasSemanaValidas {
  horasMaximasSemanales = 0;
  cumpleCon(horasObligatoriasSemana, horaInicio) {
    this.horasMaximasSemanales = HORAS_MAXIMAS_ORDINARIAS.find(
      (h) =>
        new Date(h.vigente_desde).getTime() <= new Date(horaInicio).getTime()
    )?.horas_maximas_semanales;

    return (
      Array.isArray(horasObligatoriasSemana) &&
      horasObligatoriasSemana.length === 7 &&
      horasObligatoriasSemana.every(
        (hora) => typeof hora === "number" && hora >= 0 && hora <= 24
      ) &&
      horasObligatoriasSemana.reduce((acc, curr) => acc + curr, 0) >= 0 &&
      horasObligatoriasSemana.reduce((acc, curr) => acc + curr, 0) <=
        this.horasMaximasSemanales
    );
  }
  obtenerMensajeError() {
    return `Las horas obligatorias de la semana deben ser un arreglo de 7 elementos, cada uno debe ser un número entre 0 y 24 y la suma de todas las horas debe ser mayor o igual a 0 y menor o igual a ${this.horasMaximasSemanales}`;
  }
}

export class EspecificacionFechaValida {
  cumpleCon(dateString) {
    const fechaAValidar = new Date(dateString);

    return fechaAValidar.toString() !== "Invalid Date";
  }

  obtenerMensajeError() {
    return "La fecha debe tener el formato DD/MM/YY o DD/MM/YY, HH:MM y representar una fecha válida.";
  }
}

export class EspecificacionMaximo24Horas {
  cumpleCon(horas) {
    return horas <= 24;
  }
  obtenerMensajeError() {
    return "Las duración del turno no puede ser mayor a 24 horas";
  }
}

export class EspecificacionRangosNoSuperpuestos {
  cumpleCon(registros) {
    return validarRangosNoSuperpuestos(
      registros.map((registro) => ({
        start: registro.horaInicio,
        end: registro.horaFin,
      }))
    );
  }
  obtenerMensajeError() {
    return "Los rangos no pueden superponerse";
  }
}

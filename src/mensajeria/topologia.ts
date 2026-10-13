import type { Channel, Options } from 'amqplib';

// Los tres exchanges. El tipo va al lado del nombre porque declararlo con otro tipo es un error.
export const EXCHANGES = {
  eventos: { nombre: 'biblioteca.eventos', tipo: 'topic' },
  comandos: { nombre: 'biblioteca.comandos', tipo: 'direct' },
  dlx: { nombre: 'biblioteca.dlx', tipo: 'direct' },
} as const;

// Las tres colas de trabajo.
export const COLAS = {
  notificaciones: 'notificaciones',
  auditoria: 'auditoria',
  correos: 'correos',
} as const;

// Una DLQ por cola de trabajo, nunca una compartida.
export const DLQ = {
  notificaciones: 'notificaciones.dlq',
  auditoria: 'auditoria.dlq',
  correos: 'correos.dlq',
} as const;

export const ROUTING_KEYS = {
  prestamoCreado: 'prestamo.creado',
  prestamoDevuelto: 'prestamo.devuelto',
  libroAgotado: 'libro.agotado',
  correoEnviar: 'correo.enviar',
} as const;

// Que cola escucha que, y con que patron.
export const BINDINGS = [
  { cola: COLAS.notificaciones, exchange: EXCHANGES.eventos.nombre, patron: 'prestamo.*' },
  { cola: COLAS.auditoria, exchange: EXCHANGES.eventos.nombre, patron: '#' },
  { cola: COLAS.correos, exchange: EXCHANGES.comandos.nombre, patron: ROUTING_KEYS.correoEnviar },
] as const;

// Lo que muere en una cola de trabajo va al dlx con el nombre de esa cola como routing key.
export function argumentosDeCola(cola: string): Options.AssertQueue {
  return {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': EXCHANGES.dlx.nombre,
      'x-dead-letter-routing-key': cola,
    },
  };
}

export async function declararTopologia(canal: Channel): Promise<void> {
  for (const ex of Object.values(EXCHANGES)) {
    await canal.assertExchange(ex.nombre, ex.tipo, { durable: true });
  }

  // Las DLQ primero: si no existen cuando llega el primer rechazo, el mensaje se pierde.
  for (const clave of Object.keys(COLAS) as Array<keyof typeof COLAS>) {
    await canal.assertQueue(DLQ[clave], { durable: true });
    await canal.bindQueue(DLQ[clave], EXCHANGES.dlx.nombre, COLAS[clave]);
  }

  for (const cola of Object.values(COLAS)) {
    await canal.assertQueue(cola, argumentosDeCola(cola));
  }

  for (const binding of BINDINGS) {
    await canal.bindQueue(binding.cola, binding.exchange, binding.patron);
  }
}

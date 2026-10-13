import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import type { ConsumeMessage } from 'amqplib';
import { Repository } from 'typeorm';
import { COLAS } from '../topologia.js';
import { MensajeriaService } from '../mensajeria.service.js';
import { conReintentos, detalle, esDelMensaje, MensajeInvalido, REINTENTOS } from '../reintentos.js';
import { Notificacion } from '../../persistencia/notificacion.entity.js';

@Injectable()
export class CorreosConsumidor implements OnModuleInit {
  private readonly log = new Logger(CorreosConsumidor.name);

  constructor(
    private readonly mensajeria: MensajeriaService,
    @InjectRepository(Notificacion)
    private readonly notificaciones: Repository<Notificacion>,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.mensajeria.canal.consume(
      COLAS.correos,
      (mensaje) => void this.procesar(mensaje),
      { noAck: false },
    );
    this.log.log(`escuchando la cola ${COLAS.correos}`);
  }

  private async procesar(mensaje: ConsumeMessage | null): Promise<void> {
    if (mensaje === null) return;
    const canal = this.mensajeria.canal;

    try {
      const comando = JSON.parse(mensaje.content.toString()) as Record<string, unknown>;
      const { para, asunto, origen } = comando;
      if (typeof para !== 'string' || para === '') throw new MensajeInvalido('el comando no trae para');
      if (typeof asunto !== 'string' || asunto === '') throw new MensajeInvalido('el comando no trae asunto');
      // El eventoId del evento que origino el aviso, no el x-evento-id de este comando: es el que une con eventos_auditoria.
      const eventoId = String(comando.eventoId ?? '');
      if (eventoId === '') throw new MensajeInvalido('el comando no trae el eventoId del evento de origen');

      this.log.log(`enviando correo a ${para}: "${asunto}" (origen ${String(origen)})`);

      await conReintentos(
        () => this.notificaciones.insert({ para, asunto, estado: 'enviada', eventoId }),
        (intento, error) =>
          this.log.warn(`intento ${intento} de ${REINTENTOS.maximo} fallido para ${eventoId}: ${detalle(error)}`),
      );

      canal.ack(mensaje);                           // recien aqui el broker lo borra
    } catch (error) {
      canal.nack(mensaje, false, false);            // fuera de la cola, y al DLX
      const ruta = esDelMensaje(error) ? 'mensaje invalido' : `la base no respondio en ${REINTENTOS.maximo} intentos`;
      this.log.error(`${ruta}, hacia la DLQ: ${detalle(error)}`);
    }
  }
}

// consumerDataML.js
import 'dotenv/config';
import amqp from 'amqplib';
import { processDataML } from './controller/dataML.js';
import { mlService } from './functions/apiML.js'; // <— acá


// datos de conexión fijos
const connection = await amqp.connect({
    protocol: "amqp",
    hostname: "158.69.131.226",
    port: 5672,
    username: "lightdata",
    password: "QQyfVBKRbw6fBb",
    heartbeat: 30,
});

const QUEUE = 'dataML';
const PREFETCH = 5;
const ch = await connection.createChannel();
await ch.assertQueue(QUEUE, { durable: true });
ch.prefetch(PREFETCH);

// 🔹 enviar un mensaje de prueba
const testPayload = {
    idEmpresa: 275,
    did: 10001,
    sellerId: "1964159102",
    shipmentId: "45399487764",
};
ch.sendToQueue(QUEUE, Buffer.from(JSON.stringify(testPayload)), {
    persistent: true,
    contentType: "application/json",
});
console.log("📤 Mensaje de prueba enviado:", testPayload);

// 🔹 consumir mensajes de la cola
console.log(`[AMQP] Escuchando cola "${QUEUE}"...`);

ch.consume(
    QUEUE,
    async (msg) => {
        if (!msg) return;
        let data;
        try {
            data = JSON.parse(msg.content.toString());
        } catch (e) {
            console.error("❌ JSON inválido:", e);
            ch.ack(msg);
            return;
        }

        try {
            await processDataML({ data, mlService });
            console.log("✅ Procesado:", data);
            ch.ack(msg);
        } catch (err) {
            console.error("❌ Error procesando:", err);
            ch.nack(msg, false, !msg.fields.redelivered);
        }
    },
    { noAck: false }
);

// 🔹 cierre prolijo
const close = async () => {
    try { await ch.close(); } catch { }
    try { await connection.close(); } catch { }
    process.exit(0);
};
process.on("SIGINT", close);
process.on("SIGTERM", close);

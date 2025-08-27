// testLocal.js  (ESM)
import { processDataML } from './controller/dataML.js';
import { mlService } from './functions/apiML.js';
import { redisClient } from './db.js'; // para cerrar prolijo (si tu db.js lo exporta)

const data = {
    idEmpresa: 275,        // empresa que exista en tu Redis empresasData
    did: 272,              // id del envío en tu tabla envios
    sellerId: '1964159102',
    shipmentId: '45399487764',
};

try {
    const res = await processDataML({ data, mlService });
    console.log('Resultado processDataML =>', res);
} catch (err) {
    console.error('❌ Error en processDataML:', err);
} finally {
    // cierre prolijo si tu db.js mantiene conexiones vivas
    try { await redisClient?.quit?.(); } catch { }
    // la conexión MySQL se cierra dentro de processDataML (getConnection -> finally)
}

import mysql from 'mysql';
import redis from 'redis';


export const redisClient = redis.createClient({
    socket: {
        host: '192.99.190.137',
        port: 50301,
    },
    password: 'sdJmdxXC8luknTrqmHceJS48NTyzExQg',
});

redisClient.on('error', (err) => {
    console.error('Error al conectar con Redis:', err);
});

(async () => {
    await redisClient.connect();
    //  console.log('Redis conectado');
})();

export async function getConnection(idempresa) {
    try {
        // console.log("idempresa recibido:", idempresa);

        // Validación del tipo de idempresa
        if (typeof idempresa !== 'string' && typeof idempresa !== 'number') {
            throw new Error(`idempresa debe ser un string o un número, pero es: ${typeof idempresa}`);
        }

        // Obtener las empresas desde Redis
        const redisKey = 'empresasData';
        const empresasData = await getFromRedis(redisKey);
        if (!empresasData) {
            throw new Error(`No se encontraron datos de empresas en Redis.`);
        }

        // console.log("Datos obtenidos desde Redis:", empresasData);

        // Buscar la empresa por su id
        const empresa = empresasData[String(idempresa)];
        if (!empresa) {
            throw new Error(`No se encontró la configuración de la empresa con ID: ${idempresa}`);
        }

        //   console.log("Configuración de la empresa encontrada:", empresa);

        // Configurar la conexión a la base de datos
        const config = {
            host: 'bhsmysql1.lightdata.com.ar',  // Host fijo
            database: empresa.dbname,           // Base de datos desde Redis
            user: empresa.dbuser,               // Usuario desde Redis
            password: empresa.dbpass,           // Contraseña desde Redis
        };

        //console.log("Configuración de la conexión:", config);

        return mysql.createConnection(config);
    } catch (error) {
        console.error(`Error al obtener la conexión:`, error.message);

        // Lanza un error con una respuesta estándar
        throw {
            status: 500,
            response: {
                estado: false,

                error: -1,

            },
        };
    }
}

// Función para obtener datos desde Redis
export async function getFromRedis(key) {
    try {
        const value = await redisClient.get(key);
        return value ? JSON.parse(value) : null;
    } catch (error) {
        console.error(`Error obteniendo clave ${key} de Redis:`, error);
        throw {
            status: 500,
            response: {
                estado: false,

                error: -1

            },
        };
    }
}
export async function updateEstadoRedis(empresaId, envioId, estado) {
    let DWRTE = await redisClient.get('DWRTE');
    DWRTE = DWRTE ? JSON.parse(DWRTE) : {};

    const empresaKey = `e.${empresaId}`;
    const envioKey = `en.${envioId}`;
    const ahora = Date.now(); // Guardamos el timestamp actual

    if (!DWRTE[empresaKey]) {
        DWRTE[empresaKey] = {};
    }

    if (!DWRTE[empresaKey][envioKey]) {
        DWRTE[empresaKey][envioKey] = { estado, timestamp: ahora };
    } else {
        DWRTE[empresaKey][envioKey].estado = estado;
        // No actualizamos el timestamp para no resetear la antigüedad
    }

    await redisClient.set('DWRTE', JSON.stringify(DWRTE));
}
export async function executeQuery(connection, query, values, log = false) {
    if (log) {
        logYellow(`Ejecutando query: ${query} con valores: ${values}`);
    }
    try {
        return new Promise((resolve, reject) => {
            connection.query(query, values, (err, results) => {
                if (err) {
                    if (log) {
                        logRed(`Error en executeQuery: ${err.message}`);
                    }
                    reject(err);
                } else {
                    if (log) {
                        console.log(`Resultado de la consulta: ${JSON.stringify(results)}`);

                    }
                    resolve(results);
                }
            });
        });
    } catch (error) {
        logRed(`Error en executeQuery: ${error.stack}`);
        throw error;
    }
}





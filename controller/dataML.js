// processDataML.js – versión completa
// Importa helpers compartidos
import { log } from 'node:console';
import { executeQuery, getConnection, redisClient } from '../db.js';

/**
 * Normaliza strings: quita diacríticos y raros específicos del PHP original
 * Nota: si tu runtime no soporta Unicode property escapes (\p{Diacritic}),
 * podés reemplazar por /[\u0300-\u036f]/g
 */
function normalizeStr(str = "") {
    if (str == null) return "";
    const s = String(str);
    // Remueve diacríticos
    let out = s.normalize("NFD").replace(/\p{Diacritic}/gu, "");
    // Reemplazos puntuales vistos en PHP
    out = out
        .replace(/Ŕ/g, "R")
        .replace(/ĺ/g, "l")
        .replace(/ė/g, "e")
        .replace(/ł/g, "l");
    return out;
}

function isNonEmptyStr(v) {
    return typeof v === "string" && v.trim() !== "";
}
function pickStr(...vals) {
    for (const v of vals) if (isNonEmptyStr(v)) return v;
    return "";
}
function pickId(...vals) {
    for (const v of vals) {
        if (v !== undefined && v !== null && String(v).trim() !== "") return v;
    }
    return "";
}
function pickNum(...vals) {
    for (const v of vals) {
        if (v === undefined || v === null) continue;
        const n = Number(v);
        if (!Number.isNaN(n)) return n;
    }
    return null;
}

function buildDataFromML(shipment, order) {
    const ra = shipment?.receiver_address || {};
    const so = shipment?.shipping_option || {};
    const buyer = order?.buyer || {};

    // Address (preferimos anidado; ignoramos strings vacíos de los aplanados)
    const street_name = pickStr(shipment?.receiver_address_street_name, ra.street_name);
    const street_number = pickStr(shipment?.receiver_address_street_number, ra.street_number);
    const address_line = pickStr(shipment?.receiver_address_address_line, ra.address_line);
    const phone = pickStr(shipment?.receiver_address_receiver_phone, ra.receiver_phone, ra.phone);
    const city_id = pickId(shipment?.receiver_address_city_id, ra.city_id, ra.city?.id);
    const city_name = pickStr(shipment?.receiver_address_city_name, ra.city_name, ra.city?.name);
    const state_id = pickId(shipment?.receiver_address_state_id, ra.state_id, ra.state?.id);
    const state_name = pickStr(shipment?.receiver_address_state_name, ra.state_name, ra.state?.name);
    const country_id = pickId(shipment?.receiver_address_country_id, ra.country_id, ra.country?.id);
    const country_name = pickStr(shipment?.receiver_address_country_name, ra.country_name, ra.country?.name);
    const zip_code = pickId(shipment?.receiver_address_zip_code, ra.zip_code, ra.zipcode);
    const comment = pickStr(shipment?.receiver_address_comment, ra.comment);
    const lat = pickNum(shipment?.receiver_address_latitude, ra.latitude);
    const lon = pickNum(shipment?.receiver_address_longitude, ra.longitude);
    const delivery_pref = pickStr(shipment?.delivery_preference, ra.delivery_preference);

    // Nombre del receptor (preferimos el de la address; si no, buyer)
    const receiverName = pickStr(
        shipment?.receiver_address_receiver_name,
        ra.receiver_name,
        `${buyer.first_name || ""} ${buyer.last_name || ""}`.trim()
    );

    // Peso (0 por defecto)
    const peso =
        shipment?.peso != null ? Number(shipment.peso)
            : shipment?.weight != null ? Number(shipment.weight)
                : 0;

    // Extended: serializamos si es objeto
    const extRaw =
        shipment?.shipping_option_estimated_delivery_extended ??
        shipment?.estimated_delivery_extended ??
        so?.estimated_delivery_extended ??
        null;
    const estimated_extended =
        extRaw && typeof extRaw === "object" ? JSON.stringify(extRaw) : (extRaw || "");

    return {
        // Identificadores
        order_id: shipment?.order_id || order?.id || order?.order_id || "",
        receiver_id: shipment?.receiver_id || "",
        receiver_address_id: pickId(shipment?.receiver_address_id, ra.id),

        // Receiver / Address
        receiver_address_receiver_name: receiverName,
        receiver_address_receiver_phone: phone,
        receiver_address_address_line: address_line,
        receiver_address_street_name: street_name,
        receiver_address_street_number: street_number,
        receiver_address_zip_code: zip_code,
        receiver_address_city_id: city_id,
        receiver_address_city_name: city_name,
        receiver_address_state_id: state_id,
        receiver_address_state_name: state_name,
        receiver_address_country_id: country_id,
        receiver_address_country_name: country_name,
        receiver_address_neighborhood_id: pickId(
            shipment?.receiver_address_neighborhood_id, ra.neighborhood?.id
        ),
        receiver_address_neighborhood_name: pickStr(
            shipment?.receiver_address_neighborhood_name, ra.neighborhood?.name
        ),
        receiver_address_municipality_id: pickId(
            shipment?.receiver_address_municipality_id, ra.municipality?.id
        ),
        receiver_address_municipality_name: pickStr(
            shipment?.receiver_address_municipality_name, ra.municipality?.name
        ),
        receiver_address_latitude: lat,
        receiver_address_longitude: lon,
        receiver_address_comment: comment,
        delivery_preference: delivery_pref,

        // Envío / Costos / tiempos
        peso,                                  // ✅ nunca null
        base_cost: shipment?.base_cost ?? null,
        order_cost: order?.order_cost ?? order?.base_cost ?? null,
        tracking_method: pickStr(shipment?.tracking_method, ""), // null -> ""
        tracking_number: pickStr(shipment?.tracking_number, ""),
        date_created: shipment?.date_created || order?.date_created || null,

        // Shipping option (preferimos anidado so.*)
        shipping_option_id: pickId(shipment?.shipping_option_id, so.id),
        shipping_option_shipping_method_id: pickId(shipment?.shipping_option_shipping_method_id, so.shipping_method_id),
        shipping_option_name: pickStr(shipment?.shipping_option_name, so.name),
        shipping_option_currency_id: pickStr(shipment?.shipping_option_currency_id, so.currency_id),
        shipping_option_cost: pickNum(shipment?.shipping_option_cost, so.cost),
        shipping_option_list_cost: pickNum(shipment?.shipping_option_list_cost, so.list_cost),
        shipping_option_estimated_delivery_time:
            shipment?.shipping_option_estimated_delivery_time ||
            so?.estimated_delivery_time?.date || null,
        shipping_option_estimated_delivery_limit:
            shipment?.shipping_option_estimated_delivery_limit ||
            so?.estimated_delivery_limit?.date || null,
        shipping_option_estimated_delivery_final:
            shipment?.shipping_option_estimated_delivery_final ||
            so?.estimated_delivery_final?.date || null,
        shipping_option_estimated_delivery_extended: estimated_extended, // ✅ string/JSON

        // Extras
        obs: shipment?.obs || "",
        turbo: shipment?.turbo ?? 0,
        ml_pack_id: order?.pack_id ?? "",

        // Items
        items: shipment?.items || shipment?.shipping_items || [],
        order_items: order?.order_items || [],
    };
}


/** Inserta dirección destino */
async function insertDireccionDestino(connection, didEnvio, data) {
    const q = `INSERT INTO envios_direcciones_destino 
    (didEnvio, calle, numero,address_line, cp, localidad, provincia, pais, latitud, longitud, observacion, delivery_preference,destination_comments)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`;

    const values = [
        didEnvio,
        data.receiver_address_street_name || "",
        data.receiver_address_street_number || "",
        data.receiver_address_street_name + " " + data.receiver_address_street_number || "",
        data.receiver_address_zip_code || "",
        data.receiver_address_city_name || "",
        data.receiver_address_state_name || "",
        data.receiver_address_country_name || "",
        data.receiver_address_latitude ?? null,
        data.receiver_address_longitude ?? null,
        data.receiver_address_comment || "",
        data.delivery_preference || "",
        data.receiver_address_comment || "",
    ];


    const result = await executeQuery(connection, q, values);

    const updateDid = `UPDATE envios_direcciones_destino SET did = ? WHERE id = ?`;
    await executeQuery(connection, updateDid, [result.insertId, result.insertId]);

}



/** Inserta items (solo si fullfilment aplica) */
async function insertItemsIfNeeded(connection, didEnvio, items = [], order_items = []) {
    if (!items.length) return;

    const findSellerSku = (itemId) => {
        const m = order_items.find((oi) => oi?.item?.id === itemId);
        return m?.item?.seller_sku || "";
    };

    const q = `INSERT INTO envios_items (didEnvio, codigo, imagen, descripcion, ml_id, dimensions, cantidad, variacion, seller_sku)
             VALUES (?,?,?,?,?,?,?,?,?)`;

    for (const it of items) {
        const values = [
            didEnvio,
            it.id || "",
            "", // imagen
            it.description || "",
            it.id || "",
            it.dimensions || "",
            it.quantity ?? 0,
            "", // variacion
            findSellerSku(it.id),
        ];
        await executeQuery(connection, q, values);
    }
}

/** UPDATE principal de envios (equivalente a Fguardar) */
// reemplazo seguro de updateEnvioFromData(connection, didEnvio, data)
/** Lista las columnas reales de la tabla envios (cacheable si querés) */
async function getEnviosColumns(connection) {
    const rows = await executeQuery(
        connection,
        `SELECT COLUMN_NAME 
       FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'envios'`,
        [], false
    );
    return rows.map(r => r.COLUMN_NAME);
}

function toSqlValue(v) {
    if (v === undefined) return null;
    if (v instanceof Date) return v;
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer?.(v)) return v;
    if (typeof v === 'object' && v !== null) {
        try { return JSON.stringify(v); } catch { return String(v); }
    }
    return v;
}

async function updateEnvioFromData(connection, didEnvio, data) {
    const existingCols = new Set(await getEnviosColumns(connection));

    const normalize = (s = "") => {
        const n = String(s);
        let out = n.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
        out = out.replace(/Ŕ/g, "R").replace(/ĺ/g, "l").replace(/ė/g, "e").replace(/ł/g, "l");
        return out;
    };

    const tipoEdi =
        data.delivery_preference === "residential"
            ? " (R)"
            : data.delivery_preference
                ? " (C)"
                : "";

    const destination_receiver_name = normalize(data.receiver_address_receiver_name || "");
    const destination_city_name = normalize(data.receiver_address_city_name || "");

    // Validación dura
    if (!data.order_id || !destination_city_name || !(data.receiver_address_zip_code || "").toString().trim()) {
        throw new Error("Validación: faltan order_id, ciudad o CP");
    }

    // 👇 Forzar peso a número (default 0)
    const pesoNum = Number.isFinite(Number(data.peso)) ? Number(data.peso) : 0;

    // 👇 Serializar extended si viene objeto (evita "shipping in field list")
    const extRaw =
        data.shipping_option_estimated_delivery_extended ??
        data.estimated_delivery_extended ??
        "";
    const estimated_extended =
        extRaw && typeof extRaw === "object" ? JSON.stringify(extRaw) : extRaw;

    // Mapa candidato -> valores
    const candidate = {
        ml_venta_id: data.order_id,
        obs: data.obs || "",
        peso: pesoNum, // ✅ nunca null
        tracking_method: data.tracking_method || "",
        tracking_number: data.tracking_number || "",
        fecha_venta: data.date_created ? new Date(data.date_created) : null,

        destination_type: "",
        destination_receiver_id: data.receiver_id || "",
        destination_receiver_name,
        destination_receiver_phone: data.receiver_address_receiver_phone || "",
        destination_comments: data.receiver_address_comment || "",
        destination_shipping_address_id: data.receiver_address_id || "",
        destination_shipping_address_line: (data.receiver_address_address_line || "") + tipoEdi,
        destination_shipping_street_name: data.receiver_address_street_name || "",
        destination_shipping_street_number: data.receiver_address_street_number || "",
        destination_shipping_comment: "",

        destination_shipping_zip_code: data.receiver_address_zip_code || "",
        destination_city_id: data.receiver_address_city_id || "",
        destination_city_name,
        destination_state_id: data.receiver_address_state_id || "",
        destination_state_name: data.receiver_address_state_name || "",
        destination_country_id: data.receiver_address_country_id || "",
        destination_country_name: data.receiver_address_country_name || "",
        destination_neighborhood_id: data.receiver_address_neighborhood_id || "",
        destination_neighborhood_name: data.receiver_address_neighborhood_name || "",
        destination_municipality_id: data.receiver_address_municipality_id || "",
        destination_municipality_name: data.receiver_address_municipality_name || "",
        destination_types: "",

        destination_latitude: data.receiver_address_latitude ?? null,
        destination_longitude: data.receiver_address_longitude ?? null,

        // Si tu esquema no las tiene, se ignoran más abajo
        lead_time_option_id: data.shipping_option_id || "",
        lead_time_shipping_method_id: data.shipping_option_shipping_method_id || "",
        lead_time_shipping_method_name: data.shipping_option_name || "",
        lead_time_shipping_method_type: "",
        lead_time_shipping_method_deliver_to: "",
        lead_time_currency_id: data.shipping_option_currency_id || "",
        lead_time_cost: data.shipping_option_cost ?? null,
        lead_time_list_cost: data.shipping_option_list_cost ?? null,

        estimated_delivery_time_date: data.shipping_option_estimated_delivery_time
            ? new Date(data.shipping_option_estimated_delivery_time)
            : null,
        estimated_delivery_time_date_72: data.shipping_option_estimated_delivery_limit
            ? new Date(data.shipping_option_estimated_delivery_limit)
            : null,
        estimated_delivery_time_date_480: data.shipping_option_estimated_delivery_final
            ? new Date(data.shipping_option_estimated_delivery_final)
            : null,
        estimated_delivery_extended: estimated_extended, // ✅ string/JSON

        estado: 0,
        fecha_carga: new Date(),
        base_cost: data.base_cost ?? null,
        delivery_preference: data.delivery_preference || "",
        valor_declarado: data.order_cost ?? null,
        turbo: data.turbo ?? 0,
    };
    console.log("[envios] candidate:", candidate);


    // FILTRO por columnas existentes + serialización de objetos
    const setClauses = [];
    const values = [];
    const skipped = [];

    for (const [col, val] of Object.entries(candidate)) {
        if (existingCols.has(col)) {
            setClauses.push("`" + col + "` = ?");
            values.push(toSqlValue(val)); // 👈 acá convertimos objetos a JSON string
        } else {
            skipped.push(col);
        }
    }

    if (setClauses.length === 0) {
        throw new Error("No hay columnas válidas para actualizar en envios (revisá el esquema)");
    }

    const sql = `UPDATE envios SET ${setClauses.join(", ")} WHERE superado=0 AND elim=0 AND id = ?`;
    values.push(didEnvio);

    // Logs de diagnóstico (podés apagarlos cuando quede estable)
    console.log("[envios] columnas existentes:", existingCols);
    console.log("[envios] columnas actualizadas:", setClauses.map(s => s.replace(" = ?", "")));
    console.log("[envios] columnas ignoradas:", skipped);
    console.log("[envios] SQL:", sql);
    console.log("[envios] values.length:", values.length);

    await executeQuery(connection, sql, values, /*log=*/false);
}


/** Marca el envío como estado=1 (procesado) */
async function markEnvioProcessed(connection, didEnvio) {
    await executeQuery(connection, `UPDATE envios SET estado=1 WHERE superado=0 AND elim=0 AND id=?`, [didEnvio]);
}

/** Obtiene didCliente y (por si hace falta) shipment/vendedor desde envios */
async function getEnvioRow(connection, didEnvio) {
    const rows = await executeQuery(
        connection,
        `SELECT id, ml_shipment_id, ml_vendedor_id, didCliente FROM envios WHERE id=? AND superado=0 AND elim=0 LIMIT 1`,
        [didEnvio]
    );
    return rows?.[0] || null;
}

/** Devuelve fullfilment = 0/1 para un didCliente */
async function getClienteFullfilment(connection, didCliente) {
    const rows = await executeQuery(
        connection,
        `SELECT fullfilment FROM clientes WHERE did=? AND superado=0 AND elim=0 LIMIT 1`,
        [didCliente]
    );
    const full = rows?.[0]?.fullfilment;
    return Number(full) === 1 ? 1 : 0;
}

/**
 * Función principal: procesa el mensaje de la cola y persiste datos
 * @param {Object} params
 * @param {import('mysql2/promise').Connection} params.connection
 * @param {{idEmpresa:number,did:number,sellerId?:string|number,shipmentId?:string|number}} params.data
 * @param {{getShipment:(token:string, shipmentId:string|number)=>Promise<any>, getOrder:(token:string, orderId:string|number)=>Promise<any>}} params.mlService
 * @param {Console} [params.logger]
 * @param {number} [params.idEmpresa]
 * @param {number[]} [params.afullIdEmpresasSi]
 */
export async function processDataML({
    data,
    mlService,
    logger = console,
    idEmpresa: idEmpresaParam,
    afullIdEmpresasSi = [],
} = {}) {
    const idEmpresa = idEmpresaParam ?? Number(data.idEmpresa);
    const connection = await getConnection(idEmpresa); // asumido existente en tu proyecto
    const didEnvio = Number(data.did);
    let shipmentId = String(data.shipmentId || "");
    let sellerId = String(data.sellerId || "");

    // helper para cerrar conexión (sirve tanto para pool.getConnection() como para connection directa)
    const closeConnection = async (conn) => {
        if (!conn) return;
        try {
            if (typeof conn.release === "function") await conn.release();
            else if (typeof conn.end === "function") await conn.end();
        } catch { /* noop */ }
    };

    try {
        if (!connection) throw new Error("connection requerido");
        if (!mlService?.getShipment || !mlService?.getOrder) {
            throw new Error("mlService.getShipment/getOrder requerido");
        }

        // 1) Traer datos base del envío
        const envioRow = await getEnvioRow(connection, didEnvio);
        if (!envioRow) throw new Error(`Envio id=${didEnvio} no encontrado o marcado superado/elim`);

        const didCliente = Number(envioRow.didCliente);
        shipmentId = shipmentId || String(envioRow.ml_shipment_id || "");
        sellerId = sellerId || String(envioRow.ml_vendedor_id || "");

        // 2) Regla puntual de override del vendedor
        if (didCliente === 209 && idEmpresa === 115 && sellerId === "2147483647") {
            sellerId = "2473756526";
        }

        // 3) Token por vendedor (Redis Hash "token")
        const token = await redisClient.hGet("token", sellerId);
        if (!token) throw new Error(`No hay token para sellerId=${sellerId}`);

        // 4) Llamadas a ML
        const shipment = await mlService.getShipment(token, shipmentId);
        if (!shipment || !shipment.receiver_address || !shipment.receiver_address.street_name) {
            throw new Error("Respuesta shipment incompleta (posible token inválido)");
        }

        const orderId =
            shipment.order_id ||
            (shipment.order && shipment.order.id) ||
            shipment.orderId;

        if (!orderId) throw new Error("No se pudo determinar orderId desde el shipment");

        const order = await mlService.getOrder(token, orderId);

        // 5) Unificar al contrato esperado
        const dataForSave = buildDataFromML(shipment, order);

        // 6) UPDATE envios (equivalente a Fguardar)
        await updateEnvioFromData(connection, didEnvio, dataForSave);

        // 7) Insert dirección destino
        await insertDireccionDestino(connection, didEnvio, dataForSave);

        // 8) Items si aplica fullfilment y la empresa está habilitada
        const fullfilment = await getClienteFullfilment(connection, didCliente);
        if (fullfilment === 1 && Array.isArray(afullIdEmpresasSi) && afullIdEmpresasSi.includes(idEmpresa)) {
            await insertItemsIfNeeded(
                connection,
                didEnvio,
                dataForSave.items,
                dataForSave.order_items
            );
        }

        // 9) Marcar como procesado
        await markEnvioProcessed(connection, didEnvio);

        logger.info(`Envio ${didEnvio} procesado correctamente (empresa ${idEmpresa})`);
        return { ok: true, didEnvio, idEmpresa };

    } catch (e) {
        logger.error(`Error procesando envio ${didEnvio} (empresa ${idEmpresa}): ${e.message}`);
        return { ok: false, didEnvio, idEmpresa, error: e.message };

    } finally {
        await closeConnection(connection);
    }
}


// Nota: La función es agnóstica de Rabbit; solo consume {connection, data, mlService}.
// Usa executeQuery/redisClient importados de '../db'.

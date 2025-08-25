// processDataML.js – versión completa
// Importa helpers compartidos
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

/**
 * Construye el payload data (el contrato que espera Fguardar en PHP) a partir de shipment y order
 */
function buildDataFromML(shipment, order) {
    const ra = shipment?.receiver_address || {};
    const city = ra.city || {};
    const state = ra.state || {};
    const country = ra.country || {};
    const neighborhood = ra.neighborhood || {};
    const municipality = ra.municipality || {};
    const buyer = order?.buyer || {};

    const receiverName =
        ra.receiver_name ||
        shipment?.receiver?.name ||
        `${buyer.first_name || ""} ${buyer.last_name || ""}`.trim();

    // items (si es que vienen en shipment)
    const items =
        Array.isArray(shipment?.items)
            ? shipment.items.map((it) => ({
                id: it?.id ?? it?.item?.id ?? "",
                description: it?.description ?? it?.title ?? "",
                dimensions: it?.dimensions ?? "",
                quantity: it?.quantity ?? 0,
            }))
            : [];

    return {
        // Identificadores
        order_id: shipment?.order_id || order?.id || order?.order_id || "",
        receiver_id: ra?.receiver_id || shipment?.receiver_id || "",
        receiver_address_id: ra?.id || "",

        // Receiver / Address (aplanado)
        receiver_address_receiver_name: receiverName,
        receiver_address_receiver_phone: ra?.receiver_phone || shipment?.receiver?.phone || "",
        receiver_address_address_line: ra?.address_line || "",
        receiver_address_street_name: ra?.street_name || "",
        receiver_address_street_number: ra?.street_number || "",
        receiver_address_zip_code: ra?.zip_code || ra?.zipcode || "",
        receiver_address_city_id: city?.id || "",
        receiver_address_city_name: city?.name || ra?.city_name || "",
        receiver_address_state_id: state?.id || "",
        receiver_address_state_name: state?.name || ra?.state_name || "",
        receiver_address_country_id: country?.id || "",
        receiver_address_country_name: country?.name || "",
        receiver_address_neighborhood_id: neighborhood?.id || "",
        receiver_address_neighborhood_name: neighborhood?.name || "",
        receiver_address_municipality_id: municipality?.id || "",
        receiver_address_municipality_name: municipality?.name || "",
        receiver_address_latitude: typeof ra?.latitude === "number" ? ra.latitude : null,
        receiver_address_longitude: typeof ra?.longitude === "number" ? ra.longitude : null,
        receiver_address_comment: ra?.comment || shipment?.comment || "",
        delivery_preference: shipment?.delivery_preference || ra?.delivery_preference || "",

        // Envío / costos / tiempos
        peso: shipment?.weight ?? shipment?.dimensions?.weight ?? null,
        base_cost: shipment?.base_cost ?? null,
        // order_cost: tomamos de order si viene, si no del shipping del order, si no base_cost
        order_cost: order?.order_cost ?? order?.shipping?.cost?.charge ?? order?.total_amount ?? order?.base_cost ?? null,
        tracking_method: shipment?.tracking_method || shipment?.tracking?.method || "",
        tracking_number: shipment?.tracking_number || shipment?.tracking?.number || "",
        date_created: shipment?.date_created || shipment?.creation_date || order?.date_created || null,

        // Shipping option
        shipping_option_id: shipment?.shipping_option?.id || "",
        shipping_option_shipping_method_id: shipment?.shipping_option?.shipping_method_id || shipment?.shipping_method?.id || "",
        shipping_option_name: shipment?.shipping_option?.name || shipment?.shipping_method?.name || "",
        shipping_option_currency_id: shipment?.shipping_option?.currency_id || "",
        shipping_option_cost: shipment?.shipping_option?.cost ?? null,
        shipping_option_list_cost: shipment?.shipping_option?.list_cost ?? null,
        shipping_option_estimated_delivery_time: shipment?.shipping_option?.estimated_delivery_time?.date || null,
        shipping_option_estimated_delivery_limit: shipment?.shipping_option?.estimated_delivery_time?.limit || null,
        shipping_option_estimated_delivery_final: shipment?.shipping_option?.estimated_delivery_time?.final || null,
        shipping_option_estimated_delivery_extended: shipment?.shipping_option?.estimated_delivery_time?.offset || "",

        // Extras
        obs: shipment?.obs || "",
        turbo: shipment?.turbo ?? 0,
        ml_pack_id: order?.pack_id ?? "",

        // Para items / order_items (fullfilment)
        items,
        order_items: Array.isArray(order?.order_items) ? order.order_items : [],
    };
}

/** Inserta dirección destino */
async function insertDireccionDestino(connection, didEnvio, data) {
    const q = `INSERT INTO envios_direcciones_destino 
    (didEnvio, calle, numero, cp, localidad, provincia, pais, latitud, longitud, obs, delivery_preference)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)`;

    const values = [
        didEnvio,
        data.receiver_address_street_name || "",
        data.receiver_address_street_number || "",
        data.receiver_address_zip_code || "",
        data.receiver_address_city_name || "",
        data.receiver_address_state_name || "",
        data.receiver_address_country_name || "",
        data.receiver_address_latitude ?? null,
        data.receiver_address_longitude ?? null,
        data.receiver_address_comment || "",
        data.delivery_preference || "",
    ];

    await executeQuery(connection, q, values);
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
async function updateEnvioFromData(connection, didEnvio, data) {
    const tipoEdi = data.delivery_preference === "residential" ? " (R)" : data.delivery_preference ? " (C)" : "";

    const destination_receiver_name = normalizeStr(data.receiver_address_receiver_name || "");
    const destination_city_name = normalizeStr(data.receiver_address_city_name || "");

    // Validaciones duras
    if (!data.order_id || !destination_city_name || !(data.receiver_address_zip_code || "").toString().trim()) {
        throw new Error("Validación: faltan order_id, ciudad o CP");
    }

    const fecha_carga = new Date();
    const estado = 0; // igual que en PHP (luego se marca 1 si todo salió ok)

    const q = `UPDATE envios SET 
    ml_venta_id=?,
    obs=?,
    peso=?,
    tracking_method=?,
    tracking_number=?,
    fecha_venta=?,
    destination_type=?,
    destination_receiver_id=?,
    destination_receiver_name=?,
    destination_receiver_phone=?,
    destination_comments=?,
    destination_shipping_address_id=?,
    destination_shipping_address_line=?,
    destination_shipping_street_name=?,
    destination_shipping_street_number=?,
    destination_shipping_comment=?,
    destination_shipping_zip_code=?,
    destination_city_id=?,
    destination_city_name=?,
    destination_state_id=?,
    destination_state_name=?,
    destination_country_id=?,
    destination_country_name=?,
    destination_neighborhood_id=?,
    destination_neighborhood_name=?,
    destination_municipality_id=?,
    destination_municipality_name=?,
    destination_types=?,
    destination_latitude=?,
    destination_longitude=?,
    lead_time_option_id=?,
    lead_time_shipping_method_id=?,
    lead_time_shipping_method_name=?,
    lead_time_shipping_method_type=?,
    lead_time_shipping_method_deliver_to=?,
    lead_time_currency_id=?,
    lead_time_cost=?,
    lead_time_list_cost=?,
    estimated_delivery_time_date=?,
    estimated_delivery_time_date_72=?,
    estimated_delivery_time_date_480=?,
    estimated_delivery_extended=?,
    estado=?,
    fecha_carga=?,
    base_cost=?,
    delivery_preference=?,
    valor_declarado=?,
    turbo=?
    WHERE superado=0 AND elim=0 AND id=?`;

    const values = [
        data.order_id,
        data.obs || "",
        data.peso ?? null,
        data.tracking_method || "",
        data.tracking_number || "",
        data.date_created ? new Date(data.date_created) : null,
        "", // destination_type (en PHP se deja "")
        data.receiver_id || "",
        destination_receiver_name,
        data.receiver_address_receiver_phone || "",
        data.receiver_address_comment || "",
        data.receiver_address_id || "",
        (data.receiver_address_address_line || "") + tipoEdi,
        data.receiver_address_street_name || "",
        data.receiver_address_street_number || "",
        "", // destination_shipping_comment
        data.receiver_address_zip_code || "",
        data.receiver_address_city_id || "",
        destination_city_name,
        data.receiver_address_state_id || "",
        data.receiver_address_state_name || "",
        data.receiver_address_country_id || "",
        data.receiver_address_country_name || "",
        data.receiver_address_neighborhood_id || "",
        data.receiver_address_neighborhood_name || "",
        data.receiver_address_municipality_id || "",
        data.receiver_address_municipality_name || "",
        "", // destination_types
        data.receiver_address_latitude ?? null,
        data.receiver_address_longitude ?? null,
        data.shipping_option_id || "",
        data.shipping_option_shipping_method_id || "",
        data.shipping_option_name || "",
        "", // lead_time_shipping_method_type
        "", // lead_time_shipping_method_deliver_to
        data.shipping_option_currency_id || "",
        data.shipping_option_cost ?? null,
        data.shipping_option_list_cost ?? null,
        data.shipping_option_estimated_delivery_time ? new Date(data.shipping_option_estimated_delivery_time) : null,
        data.shipping_option_estimated_delivery_limit ? new Date(data.shipping_option_estimated_delivery_limit) : null,
        data.shipping_option_estimated_delivery_final ? new Date(data.shipping_option_estimated_delivery_final) : null,
        data.shipping_option_estimated_delivery_extended || "",
        estado,
        fecha_carga,
        data.base_cost ?? null,
        data.delivery_preference || "",
        data.order_cost ?? null,
        data.turbo ?? 0,
        didEnvio,
    ];

    await executeQuery(connection, q, values);
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

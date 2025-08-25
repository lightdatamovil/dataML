
import axios from 'axios';
export async function obtenerDatosEnvioML(shipmentid, token) {
    //console.log(token);
    //console.log(shipmentid);
    // console.log("dataaa");

    try {
        const url = `https://api.mercadolibre.com/shipments/${shipmentid}`;
        console.log(url, "url");
        console.log(token, "token");
        const response = await axios.get(url, {
            headers: {
                Authorization: `Bearer ${token}`,
            },
        });

        if (response.data && response.data.id) {
            return response.data;
        } else {
            console.error(
                `No se encontraron datos válidos para el envío ${shipmentid}.`
            );
            return null;
        }
    } catch (error) {
        console.error(
            `Error al obtener datos del envío ${shipmentid} desde Mercado Libre:`,
            error.message
        );
        return null;
    }
}

export async function obtenerDatosOrderML(shipmentid, token) {
    try {
        const url = `https://api.mercadolibre.com/orders/${shipmentid}`;
        console.log(url);
        console.log(token);
        const response = await axios.get(url, {
            headers: {
                Authorization: `Bearer ${token}`,
            },
        });

        if (response.data && response.data.id) {
            return response.data;
        } else {
            console.error(
                `No se encontraron datos válidos para el envío ${shipmentid}.`
            );
            return null;
        }
    } catch (error) {
        console.error(
            `Error al obtener datos del envío ${shipmentid} desde Mercado Libre:`,
            error.message
        );
        return null;
    }
}



export const mlService = {
    getShipment: obtenerDatosEnvioML,
    getOrder: obtenerDatosOrderML,
};

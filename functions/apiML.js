// apiML.js
import axios from 'axios';

const api = axios.create({
    baseURL: 'https://api.mercadolibre.com',
    timeout: 10000,
});

export async function obtenerDatosEnvioML(shipmentId, token) {
    const { data } = await api.get(`/shipments/${shipmentId}`, {
        headers: { Authorization: `Bearer ${token}` },
    });
    return data || null;
}

export async function obtenerDatosOrderML(orderId, token) {
    const { data } = await api.get(`/orders/${orderId}`, {
        headers: { Authorization: `Bearer ${token}` },
    });
    return data || null;
}

// 🚩 wrapper con orden correcto (token primero, id después)
export const mlService = {
    getShipment: (token, shipmentId) => obtenerDatosEnvioML(shipmentId, token),
    getOrder: (token, orderId) => obtenerDatosOrderML(orderId, token),
};

export default mlService;

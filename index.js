import { Client } from '@elastic/elasticsearch';  // Importar el cliente de Elasticsearch

// Conectar con Elasticsearch
const es = new Client({ node: 'https://your-elasticsearch-endpoint' });

export const handler = async (event) => {
    // Iterar sobre los registros de DynamoDB Stream
    for (const record of event.Records) {
        console.log('Processing record: ', JSON.stringify(record, null, 2));

        // Si es una inserción de nuevo producto (INSERT)
        if (record.eventName === 'INSERT') {
            const newImage = record.dynamodb.NewImage;

            const producto = {
                tenant_id: newImage.tenant_id.S,
                producto_id: newImage.producto_id.S,
                descripcion: newImage.descripcion.S,
                nombre: newImage.nombre.S,
                precio: parseFloat(newImage.precio.N),
                stock: parseInt(newImage.stock.N)
            };

            try {
                const result = await es.index({
                    index: `productos-${producto.tenant_id}`,  // Usar tenant_id para diferenciar los índices
                    id: producto.producto_id, // Usar el producto_id como ID en Elasticsearch
                    body: producto
                });
                console.log('Nuevo producto indexado:', result);
            } catch (error) {
                console.error('Error al indexar el nuevo producto en Elasticsearch:', error);
            }

        // Si es una modificación de producto (MODIFY)
        } else if (record.eventName === 'MODIFY') {
            const newImage = record.dynamodb.NewImage;
            const oldImage = record.dynamodb.OldImage;

            const producto = {
                tenant_id: newImage.tenant_id.S,
                producto_id: newImage.producto_id.S,
                descripcion: newImage.descripcion.S,
                nombre: newImage.nombre.S,
                precio: parseFloat(newImage.precio.N),
                stock: parseInt(newImage.stock.N)
            };

            if (oldImage) {
                const precioAntiguo = parseFloat(oldImage.precio.N);
                const stockAntiguo = parseInt(oldImage.stock.N);
                
                // Si el precio o el stock han cambiado, actualizamos en Elasticsearch
                if (precioAntiguo !== producto.precio || stockAntiguo !== producto.stock) {
                    try {
                        const result = await es.index({
                            index: `productos-${producto.tenant_id}`,
                            id: producto.producto_id,
                            body: producto
                        });
                        console.log('Producto modificado y actualizado en Elasticsearch:', result);
                    } catch (error) {
                        console.error('Error al actualizar el producto en Elasticsearch:', error);
                    }
                }
            }

        // Si es una eliminación de producto (REMOVE)
        } else if (record.eventName === 'REMOVE') {
            const oldImage = record.dynamodb.OldImage;

            const producto = {
                tenant_id: oldImage.tenant_id.S,
                producto_id: oldImage.producto_id.S
            };

            try {
                const result = await es.delete({
                    index: `productos-${producto.tenant_id}`,  // Usar tenant_id para diferenciar los índices
                    id: producto.producto_id // Usar el producto_id como ID en Elasticsearch
                });
                console.log('Producto eliminado de Elasticsearch:', result);
            } catch (error) {
                console.error('Error al eliminar el producto en Elasticsearch:', error);
            }
        }
    }

    return `Processed ${event.Records.length} records.`;
};
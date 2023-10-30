SELECT 
    v.fecha,
    p.nombre_producto AS producto,
    dv.cantidad AS cantidad_vendida,
    p.precio_unitario
FROM 
    ventas v
JOIN 
    detalles_venta dv ON v.id_venta = dv.id_venta
JOIN 
    productos p ON dv.id_producto = p.id_producto
order by fecha;
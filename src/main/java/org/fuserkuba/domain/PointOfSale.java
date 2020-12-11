package org.fuserkuba.domain;

import java.util.List;

/**
 * Lugar donde se registró la venta
 * TODO: Incorporar datos que puedan ser de interés para filtar y/o segmentar como cadena de tiendas, ubicación, etc
 */
public class PointOfSale {
    protected String name;
    protected String location;
    protected List<Basket> baskets;
}

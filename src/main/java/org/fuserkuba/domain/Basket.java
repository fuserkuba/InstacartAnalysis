package org.fuserkuba.domain;

import java.util.GregorianCalendar;
import java.util.List;

/**
 * Cesta de compra
 */
public class Basket {

    protected long id;
    protected GregorianCalendar timestamp;
    protected Client client;
    protected List<Purchase> products;
    protected PointOfSale pointOfSale;
    protected long charge;
    //TODO: Revisar si es de interés considerar el método de pago en los análisis
    protected String paymentMethod;
}

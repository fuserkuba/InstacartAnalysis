package org.fuserkuba.domain;

/**
 * Relación entre la cesta y el producto adquirido
 * TODO: Incorporar información sobre descuentos, promociones utilizadas en la compra, etc
 */
public class Purchase {
    protected Product product;
    protected Basket basket;
    protected long price;
    protected long units;
    //Contexto de la compra: promoción, descuentos, navidades
    protected String context;
}

package com.masschaostheory.entity;

public class Measures {

    private String n;
    private String u;
    private float v;

    public Measures( String n, String u, float v )
    {
        this.n = n;
        this.u = u;
        this.v = v;
    }

    public String getN() {
        return n;
    }

    public void setN( String n ) {
        this.n = n;
    }

    public String getU() {
        return u;
    }

    public void setU( String u ) {
        this.u = u;
    }

    public float getV() {
        return v;
    }

    public void setV( float v ) {
        this.v = v;
    }
}

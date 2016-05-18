package com.creek.storm.target.fates;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A gaussian function implementation, used to assign the weight for each viewed products.
 */
public class Gaussian implements Serializable {

    private static final long serialVersionUID = -3710114410265759039L;
    private int               maxItems         = 40;
    private double            ga               = 1;
    private double            gb               = 0;
    private double            gc               = 80;
    private double[]          gaussianArray    = null;

    public Gaussian(){
        init();
    }

    public Gaussian(int maxItems, double a, double b, double c){
        this.maxItems = maxItems;
        this.ga = a;
        this.gb = b;
        this.gc = c;
        init();
    }

    public void init() {
        gaussianArray = new double[maxItems + 1];
        for (int i = 0; i <= maxItems; i++) {
            gaussianArray[i] = ga * Math.pow(Math.E, -1 * Math.pow(i - gb, 2) / (2 * gc));
        }
    }

    public double sum() {
        double sum = 0;
        for (int i = 0; i < gaussianArray.length; i++) {
            sum += gaussianArray[i];
        }
        return sum;
    }

    public double g(int x) {
        if (x < 0 || x > maxItems) return 0;
        else return gaussianArray[x];
    }

    /**
     * @return the maxItems
     */
    public int getMaxItems() {
        return maxItems;
    }

    /**
     * @param maxItems the maxItems to set
     */
    public void setMaxItems(int maxItems) {
        this.maxItems = maxItems;
    }

    /**
     * @return the ga
     */
    public double getGa() {
        return ga;
    }

    /**
     * @param ga the ga to set
     */
    public void setGa(double ga) {
        this.ga = ga;
    }

    /**
     * @return the gb
     */
    public double getGb() {
        return gb;
    }

    /**
     * @param gb the gb to set
     */
    public void setGb(double gb) {
        this.gb = gb;
    }

    /**
     * @return the gc
     */
    public double getGc() {
        return gc;
    }

    /**
     * @param gc the gc to set
     */
    public void setGc(double gc) {
        this.gc = gc;
    }

    /**
     * @return the gaussianArray
     */
    public double[] getGaussianArray() {
        return gaussianArray;
    }

    /**
     * @param gaussianArray the gaussianArray to set
     */
    public void setGaussianArray(double[] gaussianArray) {
        this.gaussianArray = gaussianArray;
    }

    @Override
    public String toString() {
        return "Gaussian [maxItems=" + maxItems + ", ga=" + ga + ", gb=" + gb + ", gc=" + gc + ", gaussianArray="
               + Arrays.toString(gaussianArray) + "]";
    }

    public static void main(String[] args) {
        Gaussian g = new Gaussian(40, 1, 0, 80);
        System.out.println(g.sum());

        double sum = 0;
        for (int i = 0; i < 6; i++) {
            sum += g.g(i);
        }
        System.out.println(sum);

        for (int i = 0; i < g.getGaussianArray().length; i++) {
            System.out.println(i + "\t" + String.format("%.6f", g.getGaussianArray()[i]));
        }

    }
}

package com.jsy.CreationalPattern.factory.jdgc;

/**
 * 简单工厂，产生馍的过程
 */
public class SimpleRoujiaMoFactory {

    public RoujiaMo creatRoujiaMo(String type) {
        RoujiaMo roujiaMo = null;
        switch (type) {
            case "Suan":
                roujiaMo = new ZSuanRoujiaMo();
                break;
            case "La":
                roujiaMo = new ZLaRoujiaMo();
                break;
            case "Tian":
                roujiaMo = new ZTianRoujiaMo();
                break;
            default:// 默认为酸肉夹馍
                roujiaMo = new ZSuanRoujiaMo();
                break;
        }
        return roujiaMo;
    }
}

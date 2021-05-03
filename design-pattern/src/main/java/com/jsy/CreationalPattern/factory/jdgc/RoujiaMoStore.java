package com.jsy.CreationalPattern.factory.jdgc;

/**
 * 简单工厂模式
 *
 * 肉夹馍店铺
 */
public class RoujiaMoStore {

    /**
     * 根据传入不同的类型卖不同的肉夹馍
     *
     * 现在这样的设计，虽然可以支持你卖肉夹馍了，但是有点问题，生产馍的种类和你的RoujiaMoStore耦合度太高了，
     * 如果增加几种风味，删除几种风味，你得一直修改sellRouJiaMo中的方法，所以我们需要做一定的修改，此时简单工厂模式就能派上用场了。
     */
    /*public RoujiaMo sellRoujiaMo(String type) {

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
                roujiaMo = new SuanRoujiaMo();
                break;
        }
        roujiaMo.prepare();
        roujiaMo.fire();
        roujiaMo.pack();
        return roujiaMo;

    }*/

    private SimpleRoujiaMoFactory factory;

    public RoujiaMoStore(SimpleRoujiaMoFactory factory) {
        this.factory = factory;
    }

    public RoujiaMo sellRoujiaMo(String type) {

        RoujiaMo roujiaMo = factory.creatRoujiaMo(type);
        roujiaMo.prepare();
        roujiaMo.fire();
        roujiaMo.pack();
        return roujiaMo;

    }

}

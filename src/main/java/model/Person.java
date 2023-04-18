package model;

import java.io.Serializable;

public class Person implements Serializable {
    public String NAME;
    public Integer AGE;
    public String COUNTRY;

    public Person(String name, int age, String country) {
        this.NAME = name;
        this.AGE = age;
        this.COUNTRY = country;
    }

    public Person(){}

    public Integer getAGE() {
        return AGE;
    }

    public void setAGE(Integer AGE) {
        this.AGE = AGE;
    }

    public String getCOUNTRY() {
        return COUNTRY;
    }

    public void setCOUNTRY(String COUNTRY) {
        this.COUNTRY = COUNTRY;
    }

    public String getNAME() {
        return NAME;
    }

    public void setNAME(String NAME) {
        this.NAME = NAME;
    }
}

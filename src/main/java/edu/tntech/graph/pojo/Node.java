package edu.tntech.graph.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Node extends GraphProperty {

    @JsonProperty("new")
    private boolean isNew;

    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean aNew) {
        isNew = aNew;
    }

    @Override
    public String toString() {

        return "{" +
                "id='" + id + '\'' +
                '}';
    }
}

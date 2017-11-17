package edu.tntech.graph.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class GraphProperty implements Serializable {
    protected String id;
    protected Map<String, String> attributes;
    protected String timeStamp;


    @JsonIgnore
    protected int timeStep;

    public int getTimeStep() {
        return timeStep;
    }

    public void setTimeStep(int timeStep) {
        this.timeStep = timeStep;
    }

    public String getId() {
        return id;
    }

    @JsonIgnore
    public Integer getIdNumber() {
        return Integer.parseInt(id);
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }


    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphProperty obj = (GraphProperty) o;
        return getIdNumber() == obj.getIdNumber();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}

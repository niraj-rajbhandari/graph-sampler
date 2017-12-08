package edu.tntech.graph.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import edu.tntech.graph.helper.GraphHelper;

public class Edge extends GraphProperty {

    private String source;
    private String target;

    private boolean directed;

    @JsonIgnore
    private Node sourceVertex;
    @JsonIgnore
    private Node targetVertex;

    public String getSource() {
        return source;
    }

    @JsonIgnore
    public Integer getSourceId() {
        return Integer.parseInt(source);
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    @JsonIgnore
    public Integer getTargetId() {
        return Integer.parseInt(target);
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public boolean isDirected() {
        return directed;
    }

    public void setDirected(boolean directed) {
        this.directed = directed;
    }

    public Node getSourceVertex() {
        return sourceVertex;
    }

    public void setSourceVertex(Node sourceVertex) {
        this.sourceVertex = sourceVertex;
    }

    public Node getTargetVertex() {
        return targetVertex;
    }

    public void setTargetVertex(Node targetVertex) {
        this.targetVertex = targetVertex;
    }

    @Override
    public String toString() {
        return "{" +
                "id='" + id + '\'' +
                "label='" + GraphHelper.getGraphLabel(this) + "'" +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Edge edge = (Edge) o;


        return this.getId().equals(edge.getId());
    }

    @Override
    public int hashCode() {
        return this.getId().hashCode();
//        return Objects.hash(super.hashCode(), GraphHelper.getEdgeSourceAttribute(this), GraphHelper.getEdgeTargetAttribute(this), GraphHelper.getGraphId(this));
    }
}

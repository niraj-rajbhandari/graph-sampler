package edu.tntech.graph.enumerator;

public enum GraphPropertyType {

    EDGE("edge"),
    NODE("node");

    private String itemType;

    private GraphPropertyType(String itemType) {
        this.itemType = itemType;
    }

    public String getItemType() {
        return itemType.toUpperCase();
    }
}

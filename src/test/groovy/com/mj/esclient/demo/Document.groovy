package com.mj.esclient.demo

class Document {

    public static final String INDEX_NAME = "document"

    String id
    String name
    String metadata1
    String metadata2

    Map toMap(){
        return [
                id: id,
                name: name,
                metadata1: metadata1,
                metadata2: metadata2
        ]
    }
}

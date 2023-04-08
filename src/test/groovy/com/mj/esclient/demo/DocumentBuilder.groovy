package com.mj.esclient.demo

class DocumentBuilder {

    static List<Document> buildDocuments(int count) {
        (0..(count-1)).collect {
            def randomStr = UUID.randomUUID().toString()
            new Document(
                    id: randomStr,
                    name: "name-${randomStr}",
                    metadata1: "metadata1-${randomStr}",
                    metadata2: "metadata2-${randomStr}"
            )
        }
    }

}

package com.mongodb.spark.sql.connector.write;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

public enum TruncateMode {
  DROP {
    @Override
    public void truncate(final WriteConfig writeConfig) {
      writeConfig.doWithCollection(MongoCollection::drop);
    }
  },
  DELETE_ALL {
    @Override
    public void truncate(final WriteConfig writeConfig) {
      writeConfig.doWithCollection(collection -> collection.deleteMany(new Document()));
    }
  },
  RECREATE {
    @Override
    public void truncate(final WriteConfig writeConfig) {
      MongoClient mongoClient = writeConfig.getMongoClient();
      MongoDatabase database = mongoClient.getDatabase(writeConfig.getDatabaseName());

      String collectionName = writeConfig.getCollectionName();
      Document getCollectionMeta =
          new Document()
              .append("listCollections", 1)
              .append("filter", new Document().append("name", collectionName));

      Document foundMeta = database.runCommand(getCollectionMeta);
      Document cursor = foundMeta.get("cursor", Document.class);
      List<Document> firstBatch = cursor.getList("firstBatch", Document.class);
      if (firstBatch.isEmpty()) {
        return;
      }

      Document collectionObj = firstBatch.get(0);
      Document options = collectionObj.get("options", Document.class);
      Document createCollectionWithOptions = new Document().append("create", collectionName);
      createCollectionWithOptions.putAll(options);

      MongoCollection<Document> originalCollection = database.getCollection(collectionName);
      List<Document> originalIndexes = originalCollection.listIndexes().into(new ArrayList<>());
      originalCollection.drop();

      database.runCommand(createCollectionWithOptions);
      Document createIndexes = new Document()
              .append("createIndexes", collectionName)
              .append("indexes", originalIndexes);
      // note: potentially we're missing now only params: writeConcern, commitQuorum, comment
      database.runCommand(createIndexes);
    }
  };

  public abstract void truncate(WriteConfig writeConfig);
}

db.chat_messages.aggregate([
    // Stage 1: Match messages with entity_id in duplicateHeaderIds
    { 
      $match: { 
        entity_id: { $in: [
          ObjectId("667b18e4a4527b87d9acb582"),
          ObjectId("635138152b9732e1bf6eeeba"),
          ObjectId("65436f237ab9abf72d44e865"),
          ObjectId("64886eacd01346f0fb8065a3"),
          ObjectId("66265f4d53e139f9808e3d31"),
          ObjectId("65ca427fb083c819f56fcb3a"),
          ObjectId("61926e4aa741680789a37300"),
          ObjectId("64999da9a5ee357df3ec053c")
        ]}
      }
    },
    // Stage 2: Group by entity_id and txn_id to find duplicate messages
    {
      $group: {
        _id: { entity_id: "$entity_id", txn_id: "$txn_id" },
        messageIds: { $push: "$_id" },  // Collect all message _ids with the same entity_id and txn_id
        count: { $sum: 1 }  // Count messages with the same entity_id and txn_id
      }
    },
    // Stage 3: Filter groups with more than one message (indicating duplicates)
    {
      $match: { count: { $gt: 1 } }
    },
    // Stage 4: Project to show only relevant details
    {
      $project: {
        _id: 0,
        entity_id: "$_id.entity_id",
        txn_id: "$_id.txn_id",
        duplicateMessageIds: "$messageIds",
        duplicateCount: "$count"
      }
    }
  ]);







  //in case we also want to delete that duplicate data we can write this 

  db.chat_messages.aggregate([
    // Group by txn_id to count occurrences of each txn_id
    {
      $group: {
        _id: "$txn_id",
        count: { $sum: 1 },
        messageIds: { $push: "$_id" }  // Collect message _ids for each txn_id
      }
    },
    // Find txn_ids that appear more than once (indicating duplicates)
    {
      $match: { count: { $gt: 1 } }
    },
    // Project to show txn_id and list of duplicate message _ids
    {
      $project: {
        _id: 0,
        txn_id: "$_id",
        duplicateMessageIds: { $slice: ["$messageIds", 1, { $size: "$messageIds" }] }  // Skip the first message (keep one)
      }
    }
  ]).forEach(function(doc) {
    // Delete all duplicate messages except the first one (the first one in messageIds array)
    db.chat_messages.deleteMany({
      _id: { $in: doc.duplicateMessageIds }
    });
  });
  
  
db.getCollection("${mongodb.collection.misureElettriche3M}").aggregate(
[
  {
    $project: {
      codice_fornitura: 1,
      pod: 1,
      "misure.autoletture": {
        $ifNull: ["$misure.autoletture", []]
      },
      "misure.volture": {
        $ifNull: ["$misure.misure_volture", []]
      },
      "misure.misure_orarie": {
        $ifNull: ["$misure.misure_orarie", []]
      },
      "misure.misure_mensili": {
        $ifNull: ["$misure.misure_mensili", []]
      },
      "misure.misure_non_orarie": {
        $ifNull: ["$misure.misure_non_orarie", []]
      }
    }
  },
  {
    $unwind: {
      path: "$misure.autoletture",
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $unwind: {
      path: "$misure.volture",
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $unwind: {
      path: "$misure.misure_orarie",
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $unwind: {
      path: "$misure.misure_mensili",
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $unwind: {
      path: "$misure.misure_non_orarie",
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      competenza_consumi: {
        $ifNull: [
          "$misure.autoletture.competenza_consumi",
          "$misure.volture.competenza_consumi",
          "$misure.misure_orarie.competenza_consumi",
          "$misure.misure_mensilis.competenza_consumi",
          "$misure.misure_non_orarie.competenza_consumi"
        ]
      }
    }
  },
  {
    $match: {
      competenza_consumi: {
        $exists: true
      }
    }
  },
  {
    $group: {
      _id: {
        $concat: [
          {
            $toString: "$_id"
          },
          "_",
          "$competenza_consumi"
        ]
      },
      codice_fornitura: {
        $first: "$codice_fornitura"
      },
      pod: {
        $first: "$pod"
      },
      competenza_consumi: {
        $first: {
          $toInt: "$competenza_consumi"
        }
      },
      autoletture: {
        $first: "$misure.autoletture"
      },
      volture: {
        $first: "$misure.volture"
      },
      misure_orarie: {
        $push: "$misure.misure_orarie"
      },
      misure_mensili: {
        $first: "$misure.misure_mensili"
      },
      misure_non_orarie: {
        $first: "$misure.misure_non_orarie"
      }
    }
  },
  {
    $merge: {
      into: "MisureElettriche3M_newSchema",
      whenMatched: "merge",
      whenNotMatched: "insert"
    }
  }
]
);

db.createCollection("${mongodb.collection.misureElettriche3MNewSchema}");

//add competenza_consumi as additional index
db.${mongodb.collection.misureElettriche3MNewSchema}.createIndex({ competenza_consumi: -1 }) // Descending index on "age"
MINUTE

-- from coarse
db.pollution.aggregate([
  { 
    $match: 
    { 
      time: {$gte: ISODate("2020-07-15T00:00:00.0Z"), $lt: ISODate("2020-07-16T00:00:00.0Z")}
    }
  },
   {
     $group: {
       _id: {
         year: { $year: "$time" },
         month: { $month: "$time" },
         day: { $dayOfMonth: "$time" },
         hour: { $hour: "$time" },
         minute: { $minute: "$time" },
         stationId: "$stationId",
         specie: "pm10"
       },
       time: { "$max": "$time" },
       min: { "$min": "$pm10" },
       max: { "$max": "$pm10" },
       avg: { "$avg": "$pm10" },
       count: { "$sum": 1 }
     }
   },
   { $merge: "measurementbyminute" }
]);



HOUR

-- from coarse
db.pollution.aggregate([
   {
     $group: {
       _id: {
         year: { $year: "$time" },
         month: { $month: "$time" },
         day: { $dayOfMonth: "$time" },
         hour: { $hour: "$time" },
         stationId: "$stationId",
         specie: "pm10"
       },
       "min": { "$min": "$pm10" },
       "max": { "$max": "$pm10" },
       "avg": { "$avg": "$pm10" },
       "count": { "$sum": 1 }
     }
   },
   { $merge: "measurementbyhour" }
]);

-- from minute

db.measurementbyminute.aggregate([
  {
  $project: {"_id.year": 1, "_id.month": 1, "_id.day": 1, "_id.hour": 1, "_id.minute": 1}
  }
]);

db.measurementbyminute.aggregate([
   {
     $group: {
       _id: {
         year: "$_id.year",
         month: "$_id.month",
         day: "$_id.day",
         hour: "$_id.hour",
         stationId: "$_id.stationId",
         specie: "$_id.specie"
       },
       time: { "$max": "$time" },
       min: { "$min": "$min" },
       max: { "$max": "$max" },
       avg: { "$avg": "$avg" },
       count: { "$sum": 1 }
     }
   },
   { $merge: "measurementbyhour" }
]);



DAY

-- from coarse
db.pollution.aggregate([
   {
     $group: {
       _id: {
         year: { $year: "$time" },
         month: { $month: "$time" },
         day: { $dayOfMonth: "$time" },
         stationId: "$stationId",
         specie: "pm10"
       },
       "min": { "$min": "$pm10" },
       "max": { "$max": "$pm10" },
       "avg": { "$avg": "$pm10" },
       "count": { "$sum": 1 }
     }
   },
   { $merge: "measurementbyday" }
]);

-- from hour

db.measurementbyhour.aggregate([
   {
     $group: {
       _id: {
         year: "$_id.year",
         month: "$_id.month",
         day: "$_id.day",
         stationId: "$stationId",
         specie: "$specie"
       },
       "min": { "$min": "$min" },
       "max": { "$max": "$max" },
       "avg": { "$avg": "$avg" },
       "count": { "$sum": 1 }
     }
   },
   { $merge: "measurementbyday" }
]);


MONTH

-- from day

db.measurementbyday.aggregate([
   {
     $group: {
       _id: {
         year: "$_id.year",
         month: "$_id.month",
         stationId: "$stationId",
         specie: "$specie"
       },
       "min": { "$min": "$min" },
       "max": { "$max": "$max" },
       "avg": { "$avg": "$avg" },
       "count": { "$sum": 1 }
     }
   }
]);








































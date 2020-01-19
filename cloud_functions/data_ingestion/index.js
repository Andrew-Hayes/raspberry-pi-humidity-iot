const admin = require('firebase-admin');

admin.initializeApp();
const db = admin.firestore();

exports.ingestIotData = (event, context) => {
  const pubsubMessage = event.data;
  const payload = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());
  
  let mapToUpdate = {}
  const timestamp = Math.round(new Date().getTime() / 1000)

  // updates a single key in the maps
  mapToUpdate[`humidityValues.${timestamp}`] = payload.humidity
  mapToUpdate[`temperatureValues.${timestamp}`] = payload.temperature

  // send to db
  return db.collection('humidity-iot-data').doc(event.attributes.deviceId).update(mapToUpdate)
};
const { Kafka } = require('kafkajs');

function getRandomLatitude() {
  return (Math.random() * 180 - 90).toFixed(6);
}

function getRandomLongitude() {
  return (Math.random() * 360 - 180).toFixed(6);
}

function getRandomInt(max) {
  return Math.floor(Math.random() * max);
}

const plates = ["ABC-1234", "XYZ-5678", "LMN-9999"];
const brands = ["Volkswagen"];
const models = ["Fusca", "Gol", "Polo"];

const kafka = new Kafka({
  clientId: 'meu-produtor',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

async function run() {
  await producer.connect();

  const appointmentId = getRandomInt(1000);
  const vehicleId = getRandomInt(1000);

  const plate = plates[getRandomInt(plates.length)];
  const brand = brands[getRandomInt(brands.length)];
  const modelo = models[getRandomInt(models.length)];

  const appointmentMessage = {
    id: appointmentId,
    vehicle_id: vehicleId,
    lat: getRandomLatitude(),
    long: getRandomLongitude(),
    process_date: new Date().toISOString()
  };

  const vehicleMessage = {
    id: vehicleId,
    plate: plate,
    brand: brand,
    modelo: modelo
  };

  const results = await Promise.all([
    producer.send({
      topic: 'br.com.brandao.appointment',
      messages: [{ value: JSON.stringify(appointmentMessage) }],
    }),
    producer.send({
      topic: 'br.com.brandao.vehicle',
      messages: [{ value: JSON.stringify(vehicleMessage) }],
    })
  ]);

  console.log('Mensagens enviadas com sucesso:', results);
  await producer.disconnect();
}

run().catch(console.error);

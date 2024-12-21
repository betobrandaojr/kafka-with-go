const { Kafka } = require('kafkajs');

function getRandomLatitude() {
  return (Math.random() * 180 - 90).toFixed(6);
}

function getRandomLongitude() {
  return (Math.random() * 360 - 180).toFixed(6);
}

const kafka = new Kafka({
  clientId: 'meu-produtor',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

let appointmentId = 0;

const run = async () => {
  await producer.connect();

  appointmentId += 1;
  
  const appointmentMessage = {
    id: appointmentId,
    vehicle_id: 1,
    lat: getRandomLatitude(),
    long: getRandomLongitude(),
    process_date: new Date().toISOString()
  };

  const vehicleMessage = {
    id: 1,
    plate: "ABC-1234",
    brand: "Volkswagen",
    modelo: "Fusca"
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
};

run().catch(console.error);

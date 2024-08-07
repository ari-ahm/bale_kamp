import { Client, StatusOK } from 'k6/net/grpc';
import { check } from 'k6';
import { b64encode } from 'k6/encoding';

export const options = {
  vus: 1000,
  duration: '30s',
};

const client = new Client();
client.load(['../api/proto/'], 'broker.proto');

export default function() {
  client.connect("localhost:50051", { plaintext: true });

  const data = { subject: 'Bert' ,
    body: b64encode("salam"),
    expirationSeconds: 50 };
  const response = client.invoke('/broker.Broker/Publish', data);

  check(response, {
    'status is OK': (r) => r && r.status === StatusOK,
  });

  client.close();
}

import argparse
import atexit
import json
import logging
import random
import time
import sys

import os 

os.system("pip install confluent_kafka")

from confluent_kafka import Producer


logging.basicConfig(
  format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
  
  datefmt='%Y-%m-%d %H:%M:%S',
  level=logging.INFO,
  handlers=[
      logging.FileHandler("sales_producer.log"),
      logging.StreamHandler(sys.stdout)
  ]
)

logger = logging.getLogger()

SELLERS = ['LNK', 'OMA', 'KC', 'DEN']


class ProducerCallback:
    def __init__(self, record, log_success=False):
        self.record = record
        self.log_success = log_success

    def __call__(self, err, msg):
        if err:
            logger.error('Error producing record {}'.format(self.record))
        elif self.log_success:
            logger.info('Produced {} to topic {} partition {} offset {}'.format(
                self.record,
                msg.topic(),
                msg.partition(),
                msg.offset()
            ))


def main(args):
    logger.info('Starting sales producer')
    conf = {
        'bootstrap.servers': args.bootstrap_server,
        'linger.ms': 200,
        'client.id': 'sales-1',
        'partitioner': 'murmur2_random'
    }

    producer = Producer(conf)

    atexit.register(lambda p: p.flush(), producer)

    i = 1
    while True:
        is_tenth = i % 10 == 0

        sales = {
            "seller_id": random.choice(SELLERS),
            "amount_usd": random.randrange(100, 1000),
            "sale_ts": int(time.time() * 1000)
        }

        import ast 
        # JSON_VALUE('{"a b": "true"}', '$.[''a b'']')

        # print(ast.literal_eval(str(json.dumps(sales)).replace("'",'"')))
        # print(str(json.dumps(sales)).replace("'",'"'))

        # asdads

        with open('config.json', 'rb') as f:
            file = f.read()
            data = json.loads(file)
        
        # df = pd.DataFrame(data)

        market = data[0:10]

        # producer.produce(topic=args.topic,
        #                 value=json.dumps(market), # value=json.dumps(sales), (market)
        #                 on_delivery=ProducerCallback(market, log_success=is_tenth)) # on_delivery=ProducerCallback(sales, log_success=is_tenth)), (market)
        
        producer.produce(topic=args.topic,
                        value=json.dumps(sales), # value=json.dumps(sales), (market)
                        on_delivery=ProducerCallback(sales, log_success=is_tenth))
        if is_tenth:
            producer.poll(1)
            time.sleep(0) # time.sleep(5)
            i = 0 

        i += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', default='localhost:9092')
    parser.add_argument('--topic', default='sales-usd')
    args = parser.parse_args()
    main(args)
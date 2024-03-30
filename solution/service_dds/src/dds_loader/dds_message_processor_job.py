import uuid
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger
                 ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            # Парсинг сообщения

            order_msg = msg['payload']

            order_id = order_msg['id']
            order_dt = order_msg['date']
            cost = order_msg['cost']
            payment = order_msg['payment']
            status = order_msg['status']
            h_order_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(order_id))
            hk_order_cost_hashdiff = uuid.uuid5(uuid.NAMESPACE_OID, str(load_dt) + str(h_order_pk))
            hk_order_status_hashdiff = uuid.uuid5(uuid.NAMESPACE_OID, str(load_dt) + str(h_order_pk))

            user_id = order_msg['user']['id']
            username = order_msg['user']['name']
            userlogin = order['user']['login']
            h_user_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(user_id))
            hk_user_names_hashdiff = uuid.uuid5(uuid.NAMESPACE_OID, str(load_dt) + str(h_user_pk))
            hk_order_user_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(h_order_pk) + str(h_user_pk))

            restaurant_id = order_msg['restaurant']['id']
            restaurant_name = order_msg['restaurant']['name']
            h_restaurant_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(restaurant_id))
            hk_restaurant_names_hashdiff = uuid.uuid5(uuid.NAMESPACE_OID, str(load_dt) + str(h_restaurant_pk))

            products = order_msg['products']

            # Раскладывание в Data Vault

            load_dt = datetime.utcnow()
            load_src = 'stg-service-orders'

            self._dds_repository.insert_dds_h_order(
                h_order_pk, order_id, order_dt, load_dt, load_src
            )

            self._dds_repository.insert_dds_s_order_cost(
                hk_order_cost_hashdiff, h_order_pk, cost, payment, load_dt, load_src
            )

            self._dds_repository.insert_dds_s_order_status(
                hk_order_status_hashdiff, h_order_pk, status, load_dt, load_src
            )

            self._dds_repository.insert_dds_h_user(
                h_user_pk, user_id, load_dt, load_src
            )

            self._dds_repository.insert_dds_s_user_names(
                hk_user_names_hashdiff, h_user_pk, username, userlogin, load_dt, load_src
            )

            self._dds_repository.insert_dds_l_order_user(
                hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src
            )

            self._dds_repository.insert_dds_h_restaurant(
                h_restaurant_pk, restaurant_id, load_dt, load_src
            )

            self._dds_repository.insert_dds_s_restaurant_names(
                hk_restaurant_names_hashdiff, h_restaurant_pk, restaurant_name, load_dt, load_src
            )

            for product in products:
                product_id = product['id']
                product_name = product['name']
                category_name = product['category']
                h_product_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(product_id))
                h_category_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(category_name))
                hk_order_product_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(h_order_pk) + str(h_product_pk))
                hk_product_category_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(h_product_pk) + str(h_category_pk))
                hk_product_restaurant_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(h_product_pk) + str(h_restaurant_pk))

                self._dds_repository.insert_dds_h_product(
                    h_product_pk, product_id, load_dt, load_src
                )

                self._dds_repository.insert_dds_s_product_names(
                    hk_product_names_hashdiff, h_product_pk, product_name, load_dt, load_src
                )

                self._dds_repository.insert_dds_l_order_product(
                    hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src
                )

                self._dds_repository.insert_dds_l_product_category(
                    hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src
                )

                self._dds_repository.insert_dds_l_product_restaurant(
                    hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src
                )

                self._dds_repository.insert_dds_h_category(
                    h_category_pk, category_name, load_dt, load_src
                )

                # Отправка сообщения для Data Mart
                dst_msg = {
                    "user_id": h_user_pk,
                    "product_id": h_product_pk,
                    "product_name": product_name,
                    "category_id": h_category_pk,
                    "category_name": category_name
                }

                self._producer.produce(dst_msg)
                self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
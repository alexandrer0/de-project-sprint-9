from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger
                 ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            # Парсинг сообщения

            user_id = msg['user_id']
            product_id = msg['product_id']
            product_name = msg['product_name']
            category_id = msg['category_id']
            category_name = msg['category_name']

            # Раскладывание в Data Mart

            self._cdm_repository.insert_cdm_user_product_counters(
                user_id, product_id, product_name
            )

            self._cdm_repository.insert_cdm_user_category_counters(
                user_id, category_id, category_name
            )

        self._logger.info(f"{datetime.utcnow()}: FINISH")
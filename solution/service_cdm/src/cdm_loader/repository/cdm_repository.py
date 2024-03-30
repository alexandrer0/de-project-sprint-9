from lib.pg import PgConnect

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_cdm_user_category_counters(self,
        user_id: str, category_id: str, category_name: str
        ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters(
                            user_id, category_id, category_name, order_cnt
                        )
                        VALUES(
                            %(user_id)s, %(category_id)s, %(category_name)s, 1
                        )
                        ON CONFLICT (user_id, category_id) DO UPDATE
                        SET
                            order_cnt = user_category_counters.order_cnt + 1
                        ;
                    """,
                    {
                        'user_id': user_id,
                        'category_id': category_id,
                        'category_name': category_name
                    }
                )

    def insert_cdm_user_product_counters(self,
        user_id: str, product_id: str, product_name: str
        ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters(
                            user_id, product_id, product_name, order_cnt
                        )
                        VALUES(
                            %(user_id)s, %(product_id)s, %(product_name)s, 1
                        )
                        ON CONFLICT (user_id, product_id) DO UPDATE
                        SET
                            order_cnt = user_product_counters.order_cnt + 1
                        ;
                    """,
                    {
                        'user_id': user_id,
                        'product_id': product_id,
                        'product_name': product_name
                    }
                )
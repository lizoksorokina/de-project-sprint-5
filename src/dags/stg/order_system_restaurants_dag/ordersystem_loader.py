from examples.stg.order_system_restaurants_dag.ordersystem_reader import MongoReader
from lib import PgConnect
from lib.dict_util import json2str
from examples.stg.order_system_restaurants_dag.pg_saver import PgSaver
from examples.stg import EtlSetting, StgEtlSettingsRepository
from logging import Logger
from datetime import datetime

class BaseLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = None
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader, get_documents_method, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.get_documents_method = get_documents_method  # метод для получения документов
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            # Прочитываем состояние загрузки
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()}
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.get_documents_method(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d["update_ts"], d)
                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")
            return len(load_queue)


class RestaurantLoader(BaseLoader):
    WF_KEY = "ordersystem_restaurants_origin_to_stg_workflow"

    def __init__(self, collection_loader: MongoReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        super().__init__(collection_loader, collection_loader.get_documents, pg_dest, pg_saver, logger)


class UserLoader(BaseLoader):
    WF_KEY = "ordersystem_users_origin_to_stg_workflow"

    def __init__(self, collection_loader: MongoReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        super().__init__(collection_loader, collection_loader.get_documents, pg_dest, pg_saver, logger)


class OrderLoader(BaseLoader):
    WF_KEY = "ordersystem_orders_origin_to_stg_workflow"

    def __init__(self, collection_loader: MongoReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        super().__init__(collection_loader, collection_loader.get_documents, pg_dest, pg_saver, logger)

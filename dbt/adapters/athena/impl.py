import agate
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.manifest import Manifest
from dbt.logger import GLOBAL_LOGGER as logger

import dbt
from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.relation import AthenaRelation


class AthenaAdapter(SQLAdapter):
    ConnectionManager = AthenaConnectionManager
    Relation = AthenaRelation

    @classmethod
    def date_function(cls):
        return 'datenow()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "STRING"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "DOUBLE" if decimals else "INT"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "TIMESTAMP"

    def drop_schema(self, database, schema, model_name=None):
        """On Presto, 'cascade' isn't supported so we have to manually cascade.

        Fortunately, we don't have to worry about cross-schema views because
        views (on hive at least) are non-binding.
        """
        relations = self.list_relations(
            database=database,
            schema=schema,
            model_name=model_name
        )
        for relation in relations:
            self.drop_relation(relation, model_name=model_name)
        super(AthenaAdapter, self).drop_schema(
            database=database,
            schema=schema,
            model_name=model_name
        )

    def _relations_cache_for_schemas(self, manifest: Manifest) -> None:
        """Populate the relations cache for the given schemas. Returns an
        iterable of the schemas populated, as strings.
        """
        if not dbt.flags.USE_CACHE:
            return

        info_schema_name_map = self._get_cache_schemas(manifest,
                                                       exec_only=True)

        for relation in self.list_relations_without_caching('INFORMATION_SCHEMA', ".*"):
            logger.debug("add relation to cache: {}".format(relation))
            self.cache.add(relation)

        # it's possible that there were no relations in some schemas. We want
        # to insert the schemas we query into the cache's `.schemas` attribute
        # so we can check it later
        self.cache.update_schemas(info_schema_name_map.schemas_searched())

# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#   Quan Zhou <quan@bitergia.com>
#   Miguel Ángel Fernández <mafesan@bitergia.com>
#

import json
import logging
import requests
import time

from datetime import timedelta
from dateutil.relativedelta import relativedelta

from importlib.resources import files

from opensearchpy import OpenSearch as ES, RequestsHttpConnection
from geopy.geocoders import Nominatim

from grimoirelab_toolkit.datetime import datetime_utcnow, str_to_datetime

from ..elastic import ElasticSearch
from ..elastic_items import HEADER_JSON
from .study_ceres_onion import ESOnionConnector, onion_study
from .graal_study_evolution import get_to_date, get_unique_repository
from statsmodels.duration.survfunc import SurvfuncRight

from .utils import anonymize_url, EXTRA_PREFIX

logger = logging.getLogger(__name__)


class Studies:
    def __init__(self, elastic, requests_session):
        self.elastic = elastic
        self.requests = requests_session
        self.ONION_INTERVAL = 3600 * 24 * 7

    def enrich_onion(
        self,
        enrich_backend,
        alias,
        in_index,
        out_index,
        data_source,
        contribs_field,
        timeframe_field,
        sort_on_field,
        seconds=None,
        no_incremental=False,
    ):

        if seconds is None:
            seconds = self.ONION_INTERVAL

        log_prefix = "[" + data_source + "] study onion"

        # out_index contains the current date to avoid removing the
        # previous index until the new one is ready
        new_index = out_index + "_" + datetime_utcnow().strftime("%Y%m%d")

        logger.info(
            "{}  starting study - Input: {} Output: {}".format(
                log_prefix, in_index, new_index
            )
        )

        # Creating connections
        es = ES(
            [enrich_backend.elastic.url],
            retry_on_timeout=True,
            timeout=100,
            verify_certs=self.elastic.requests.verify,
            connection_class=RequestsHttpConnection,
            ssl_show_warn=self.elastic.requests.verify,
        )

        in_conn = ESOnionConnector(
            es_conn=es,
            es_index=in_index,
            contribs_field=contribs_field,
            timeframe_field=timeframe_field,
            sort_on_field=sort_on_field,
        )
        out_conn = ESOnionConnector(
            es_conn=es,
            es_index=new_index,
            contribs_field=contribs_field,
            timeframe_field=timeframe_field,
            sort_on_field=sort_on_field,
            read_only=False,
        )
        old_conn = ESOnionConnector(
            es_conn=es,
            es_index=f"{out_index}*",
            contribs_field=contribs_field,
            timeframe_field=timeframe_field,
            sort_on_field=sort_on_field,
            read_only=True,
        )

        if not in_conn.exists():
            logger.info("{} missing index {}".format(log_prefix, in_index))
            return

        # Check last execution date
        latest_date = old_conn.latest_enrichment_date()
        if latest_date:
            logger.info(
                "{} Latest enrichment date: {}".format(
                    log_prefix, latest_date.isoformat()
                )
            )
            update_after = latest_date + timedelta(seconds=seconds)
            logger.info(
                "{} update after date: {}".format(log_prefix, update_after.isoformat())
            )
            if update_after >= datetime_utcnow():
                logger.info(
                    "{} too soon to update. Next update will be at {}".format(
                        log_prefix, update_after.isoformat()
                    )
                )
                return

        # Onion currently does not support incremental option
        logger.info("{} Creating out ES index".format(log_prefix))
        # Initialize out index
        if not self.elastic.is_legacy():
            filename = files("grimoire_elk").joinpath(
                "enriched/mappings/onion_es7.json"
            )
        else:
            filename = files("grimoire_elk").joinpath("enriched/mappings/onion.json")

        out_conn.create_index(filename, delete=out_conn.exists())

        onion_study(in_conn=in_conn, out_conn=out_conn, data_source=data_source)

        # Create alias if output index exists (index is always created from scratch, so
        # alias need to be created each time)
        if out_conn.exists() and not out_conn.exists_alias(out_index, alias):
            logger.info("{} Creating alias: {}".format(log_prefix, alias))
            out_conn.create_alias(alias)

        # Remove old indices
        indices = es.cat.indices(index=f"{out_index}*", format="json")
        for index in indices:
            index_name = index["index"]
            if index_name != new_index:
                logger.info("{} Removing old index: {}".format(log_prefix, index_name))
                es.indices.delete(index=index_name, ignore=[400, 404])

        logger.info("{} end".format(log_prefix))

    def enrich_extra_data(
        self, ocean_backend, enrich_backend, json_url, target_index=None
    ):
        """
        This study enables setting/removing extra fields on/from a target index.
        """
        index_url = (
            "{}/{}".format(enrich_backend.elastic_url, target_index)
            if target_index
            else enrich_backend.elastic.index_url
        )
        url = "{}/_update_by_query?wait_for_completion=true&conflicts=proceed".format(
            index_url
        )

        res = self.requests.get(index_url)
        if res.status_code != 200:
            logger.error(
                "[enrich-extra-data] Target index {} doesn't exists, "
                "study finished".format(anonymize_url(url))
            )
            return

        res = self.requests.get(json_url)
        res.raise_for_status()
        extras = res.json()

        for extra in extras:
            conds = []
            fltrs = []
            stmts = []

            # create AND conditions
            conditions = extra.get("conditions", [])
            for c in conditions:
                c_field = c["field"]
                c_value = c["value"]
                cond = {"term": {c_field: c_value}}
                conds.append(cond)

            # create date filter
            date_range = extra.get("date_range", [])
            if date_range:
                gte = date_range.get("start", None)
                lte = date_range.get("end", None)
                # handle empty values
                lte = "now" if not lte else lte

                date_fltr = {"range": {date_range["field"]: {"gte": gte, "lte": lte}}}

                fltrs.append(date_fltr)

            # populate painless, add/modify statements
            add_fields = extra.get("set_extra_fields", [])
            for a in add_fields:
                a_field = "{}_{}".format(EXTRA_PREFIX, a["field"])
                a_value = a["value"]

                if isinstance(a_value, int) or isinstance(a_value, float):
                    if isinstance(a_value, bool):
                        stmt = "ctx._source.{} = {}".format(
                            a_field, str(a_value).lower()
                        )
                    else:
                        stmt = "ctx._source.{} = {}".format(a_field, a_value)
                else:
                    stmt = "ctx._source.{} = '{}'".format(a_field, a_value)
                stmts.append(stmt)

            # populate painless, remove statements
            remove_fields = extra.get("remove_extra_fields", [])
            for r in remove_fields:
                r_field = "{}_{}".format(EXTRA_PREFIX, r["field"])

                stmt = "ctx._source.remove('{}')".format(r_field)

                stmts.append(stmt)

            es_query = """
                    {
                      "script": {
                        "source":
                        "%s",
                        "lang": "painless"
                      },
                      "query": {
                        "bool": {
                          "must": %s,
                          "filter": %s
                        }
                      }
                    }
                    """ % (
                ";".join(stmts),
                json.dumps(conds),
                json.dumps(fltrs),
            )

            try:
                r = self.requests.post(
                    url, data=es_query, headers=HEADER_JSON, verify=False
                )
            except requests.exceptions.RetryError:
                logger.warning(
                    "[enrich-extra-data] Retry exceeded while executing study. "
                    "The following query is skipped {}.".format(es_query)
                )
                continue

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                logger.error(
                    "[enrich-extra-data] Error while executing study. Study aborted."
                )
                logger.error(ex)
                return

            logger.info(
                "[enrich-extra-data] Target index {} updated with data from {}".format(
                    anonymize_url(url), json_url
                )
            )

    def find_geo_point_in_index(
        self, es_in, in_index, location_field, location_value, geolocation_field
    ):
        """Look for a geo point in the `in_index` based on the `location_field` and `location_value`"""
        query_location_geo_point = """
        {
          "_source": {
            "includes": ["%s"]
            },
          "size": 1,
          "query": {
            "bool": {
              "must": [
                {
                  "exists": {
                    "field": "%s"
                  }
                }
              ],
              "filter": [
                {"term" : { "%s" : "%s" }}
              ]
            }
          }
        }
        """ % (
            geolocation_field,
            geolocation_field,
            location_field,
            location_value,
        )

        location_geo_point = es_in.search(
            index=in_index, body=query_location_geo_point
        )["hits"]["hits"]
        geo_point_found = (
            location_geo_point[0]["_source"][geolocation_field]
            if location_geo_point
            else None
        )

        return geo_point_found

    def add_geo_point_in_index(
        self,
        enrich_backend,
        geolocation_field,
        loc_lat,
        loc_lon,
        location_field,
        location,
    ):
        """Add geo point information (`loc_lat` and `loc_lon`) to the `in_index` in `geolocation_field` based
        on the `location_field` and `location_value`."""
        es_query = """
            {
              "script": {
                "source":
                "ctx._source.%s = [:];ctx._source.%s.lat = params.geo_lat;ctx._source.%s.lon = params.geo_lon;",
                "lang": "painless",
                "params": {
                    "geo_lat": "%s",
                    "geo_lon": "%s"
                }
              },
              "query": {
                "term": {
                  "%s": "%s"
                }
              }
            }
        """ % (
            geolocation_field,
            geolocation_field,
            geolocation_field,
            loc_lat,
            loc_lon,
            location_field,
            location,
        )

        r = self.requests.post(
            enrich_backend.elastic.index_url
            + "/_update_by_query?wait_for_completion=true&conflicts=proceed",
            data=es_query.encode("utf-8"),
            headers=HEADER_JSON,
            verify=False,
        )

        r.raise_for_status()

    def enrich_geolocation(
        self, ocean_backend, enrich_backend, location_field, geolocation_field
    ):
        """
        This study includes geo points information (latitude and longitude) based on the value of
        the `location_field`.
        """
        data_source = enrich_backend.__class__.__name__.split("Enrich")[0].lower()
        log_prefix = "[{}] Geolocation".format(data_source)
        logger.info(
            "{} starting study {}".format(
                log_prefix, anonymize_url(self.elastic.index_url)
            )
        )

        es_in = ES(
            [enrich_backend.elastic_url],
            retry_on_timeout=True,
            timeout=100,
            verify_certs=self.elastic.requests.verify,
            connection_class=RequestsHttpConnection,
            ssl_show_warn=self.elastic.requests.verify,
        )
        in_index = enrich_backend.elastic.index

        query_locations_no_geo_points = """
        {
          "size": 0,
          "aggs": {
            "locations": {
              "terms": {
                "field": "%s",
                "size": 5000,
                "order": {
                  "_count": "desc"
                }
              }
            }
          },
          "query": {
            "bool": {
              "must_not": [
                {
                  "exists": {
                    "field": "%s"
                  }
                }
              ]
            }
          }
        }
        """ % (
            location_field,
            geolocation_field,
        )

        locations_no_geo_points = es_in.search(
            index=in_index, body=query_locations_no_geo_points
        )
        locations = [
            loc["key"]
            for loc in locations_no_geo_points["aggregations"]["locations"].get(
                "buckets", []
            )
        ]
        geolocator = Nominatim(user_agent="grimoirelab-elk")

        for location in locations:
            # Default lat and lon coordinates point to the Null Island https://en.wikipedia.org/wiki/Null_Island
            loc_lat = 0
            loc_lon = 0

            # look for the geo point in the current index
            loc_info = self.find_geo_point_in_index(
                es_in, in_index, location_field, location, geolocation_field
            )
            if loc_info:
                loc_lat = loc_info["lat"]
                loc_lon = loc_info["lon"]
            else:
                try:
                    loc_info = geolocator.geocode(location)
                except Exception as ex:
                    logger.debug(
                        "{} Location {} not found for {}. {}".format(
                            log_prefix,
                            location,
                            anonymize_url(enrich_backend.elastic.index_url),
                            ex,
                        )
                    )
                    continue

                # The geolocator may return a None value
                if loc_info:
                    loc_lat = loc_info.latitude
                    loc_lon = loc_info.longitude

            try:
                self.add_geo_point_in_index(
                    enrich_backend,
                    geolocation_field,
                    loc_lat,
                    loc_lon,
                    location_field,
                    location,
                )
            except requests.exceptions.HTTPError as ex:
                logger.error(
                    "{} error executing study for {}. {}".format(
                        log_prefix, anonymize_url(enrich_backend.elastic.index_url), ex
                    )
                )

        logger.info(
            "{} end {}".format(log_prefix, anonymize_url(self.elastic.index_url))
        )

    def enrich_forecast_activity(
        self,
        ocean_backend,
        enrich_backend,
        out_index,
        gelk_version,
        enrich_class_name,
        get_field_unique_id_func,
        get_grimoire_fields_func,
        observations=20,
        probabilities=[0.5, 0.7, 0.9],
        interval_months=6,
        date_field="metadata__updated_on",
    ):
        """
        The goal of this study is to forecast the contributor activity based on their past contributions.
        """
        logger.info("[enrich-forecast-activity] Start study")

        es_in = ES(
            [enrich_backend.elastic_url],
            retry_on_timeout=True,
            timeout=100,
            verify_certs=self.elastic.requests.verify,
            connection_class=RequestsHttpConnection,
            ssl_show_warn=self.elastic.requests.verify,
        )
        in_index = enrich_backend.elastic.index

        unique_repos = es_in.search(index=in_index, body=get_unique_repository())

        repositories = [
            repo["key"]
            for repo in unique_repos["aggregations"]["unique_repos"].get("buckets", [])
        ]
        current_month = datetime_utcnow().replace(day=1, hour=0, minute=0, second=0)

        logger.info(
            "[enrich-forecast-activity] {} repositories to process".format(
                len(repositories)
            )
        )
        es_out = ElasticSearch(enrich_backend.elastic.url, out_index)
        es_out.add_alias("forecast_activity_study")

        num_items = 0
        ins_items = 0

        survived_authors = []
        # iterate over the repositories
        for repository_url in repositories:
            logger.debug(
                "[enrich-forecast-activity] Start analysis for {}".format(
                    repository_url
                )
            )
            from_month = get_to_date(
                es_in, in_index, out_index, repository_url, interval_months
            )
            to_month = from_month.replace(
                month=int(interval_months), day=1, hour=0, minute=0, second=0
            )

            # analyse the repository on a given time frame
            while to_month < current_month:

                from_month_iso = from_month.isoformat()
                to_month_iso = to_month.isoformat()

                # get authors
                authors = es_in.search(
                    index=in_index,
                    body=self.authors_between_dates(
                        repository_url,
                        from_month_iso,
                        to_month_iso,
                        date_field=date_field,
                    ),
                )["aggregations"]["authors"].get("buckets", [])

                # get author activity
                for author in authors:
                    author_uuid = author["key"]
                    activities = es_in.search(
                        index=in_index,
                        body=self.author_activity(
                            repository_url,
                            from_month_iso,
                            to_month_iso,
                            author_uuid,
                            date_field=date_field,
                        ),
                    )["hits"]["hits"]

                    dates = [
                        str_to_datetime(a["_source"][date_field]) for a in activities
                    ]
                    durations = self.dates_to_duration(dates, window_size=observations)

                    if len(durations) < observations:
                        continue

                    repository_name = repository_url.split("/")[-1]
                    author_name = activities[0]["_source"].get("author_name", None)
                    author_user_name = activities[0]["_source"].get(
                        "author_user_name", None
                    )
                    author_org_name = activities[0]["_source"].get(
                        "author_org_name", None
                    )
                    author_domain = activities[0]["_source"].get("author_domain", None)
                    author_bot = activities[0]["_source"].get("author_bot", None)
                    to_month_iso = to_month.isoformat()
                    survived_author = {
                        "uuid": "{}_{}_{}_{}".format(
                            to_month_iso, repository_name, interval_months, author_uuid
                        ),
                        "origin": repository_url,
                        "repository": repository_name,
                        "interval_months": interval_months,
                        "from_date": from_month_iso,
                        "to_date": to_month_iso,
                        "study_creation_date": from_month_iso,
                        "author_uuid": author_uuid,
                        "author_name": author_name,
                        "author_bot": author_bot,
                        "author_user_name": author_user_name,
                        "author_org_name": author_org_name,
                        "author_domain": author_domain,
                        "metadata__gelk_version": gelk_version,
                        "metadata__gelk_backend_name": enrich_class_name,
                        "metadata__enriched_on": datetime_utcnow().isoformat(),
                    }

                    survived_author.update(
                        get_grimoire_fields_func(
                            survived_author["study_creation_date"], "survived"
                        )
                    )

                    last_activity = dates[-1]
                    surv = SurvfuncRight(durations, [1] * len(durations))
                    for prob in probabilities:
                        pred = surv.quantile(float(prob))
                        pred_field = "prediction_{}".format(str(prob).replace(".", ""))

                        survived_author[pred_field] = int(pred)
                        next_activity_field = "next_activity_{}".format(
                            str(prob).replace(".", "")
                        )
                        survived_author[next_activity_field] = (
                            last_activity + timedelta(days=int(pred))
                        ).isoformat()

                    survived_authors.append(survived_author)

                    if len(survived_authors) >= self.elastic.max_items_bulk:
                        num_items += len(survived_authors)
                        ins_items += es_out.bulk_upload(
                            survived_authors, get_field_unique_id_func()
                        )
                        survived_authors = []

                from_month = to_month
                to_month = to_month + relativedelta(months=+interval_months)

                logger.debug(
                    "[enrich-forecast-activity] End analysis for {}".format(
                        repository_url
                    )
                )

        if len(survived_authors) > 0:
            num_items += len(survived_authors)
            ins_items += es_out.bulk_upload(
                survived_authors, get_field_unique_id_func()
            )

        logger.info("[enrich-forecast-activity] End study")

    def dates_to_duration(self, dates, *, window_size=20):
        """
        Convert a list of dates into a list of durations
        (between consecutive dates). The resulting list is composed of
        'window_size' durations.
        """
        dates = sorted(set(dates))
        kept = dates[-window_size - 1:]  # -1 because intervals vs. bounds
        durations = []
        for first, second in zip(kept[:-1], kept[1:]):
            duration = (second - first).days
            durations.append(duration)

        return durations

    @staticmethod
    def authors_between_dates(
        repository_url,
        min_date,
        max_date,
        author_field="author_uuid",
        date_field="metadata__updated_on",
    ):
        """
        Get all authors between a min and max date
        """
        es_query = """
            {
              "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "origin": "%s"
                            }
                        },
                        {
                            "range": {
                                "%s": {
                                    "gte": "%s",
                                    "lte": "%s"
                                }
                            }
                        }
                    ]
                }
              },
              "size": 0,
              "aggs": {
                "authors": {
                    "terms": {
                        "field": "%s",
                        "order": {
                            "_key": "asc"
                        }
                    }
                }
              }
            }
            """ % (
            repository_url,
            date_field,
            min_date,
            max_date,
            author_field,
        )

        return es_query

    @staticmethod
    def author_activity(
        repository_url,
        min_date,
        max_date,
        author_value,
        author_field="author_uuid",
        date_field="metadata__updated_on",
    ):
        """
        Get the author's activity between two dates
        """
        es_query = """
                {
                  "_source": ["%s", "author_name", "author_org_name", "author_bot",
                              "author_user_name", "author_domain"],
                  "size": 5000,
                  "query": {
                    "bool": {
                        "filter": [
                            {
                                "term": {
                                    "origin": "%s"
                                }
                            },
                            {
                                "range": {
                                    "%s": {
                                        "gte": "%s",
                                        "lte": "%s"
                                    }
                                }
                            },
                            {
                                "term": {
                                    "%s": "%s"
                                }
                            }
                        ]
                    }
                  },
                  "sort": [
                        {
                            "%s": {
                                "order": "asc"
                            }
                        }
                    ]
                }
                """ % (
            date_field,
            repository_url,
            date_field,
            min_date,
            max_date,
            author_field,
            author_value,
            date_field,
        )

        return es_query

    def enrich_demography_contribution(
        self,
        ocean_backend,
        enrich_backend,
        alias,
        date_field="grimoire_creation_date",
        author_field="author_uuid",
    ):
        """
        Run demography study for the different types of the author activities and add the resulting enriched items.
        """
        data_source = enrich_backend.__class__.__name__.split("Enrich")[0].lower()
        log_prefix = "[{}] Demography Contribution".format(data_source)
        logger.info(
            "{} starting study {}".format(
                log_prefix, anonymize_url(self.elastic.index_url)
            )
        )

        es_query = self.fetch_contribution_types()
        r = self.requests.post(
            self.elastic.index_url + "/_search",
            data=es_query,
            headers=HEADER_JSON,
            verify=False,
        )
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.error(
                "{} error getting contribution types. Aborted.".format(log_prefix)
            )
            logger.error(ex)
            return

        # Obtain the list of contribution types
        type_fields = []
        for type_field in r.json()["aggregations"]["uniq_gender"]["buckets"]:
            type_fields.append(type_field["key"])

        # Run demography study for each contribution type
        type_fields.sort()
        for field in type_fields:
            self.run_demography(
                date_field, author_field, log_prefix, contribution_type=field
            )

        if not self.elastic.alias_in_use(alias):
            logger.info("{} Creating alias: {}".format(log_prefix, alias))
            self.elastic.add_alias(alias)

        logger.info(
            "{} end {}".format(log_prefix, anonymize_url(self.elastic.index_url))
        )

    def enrich_demography(
        self,
        ocean_backend,
        enrich_backend,
        alias,
        date_field="grimoire_creation_date",
        author_field="author_uuid",
    ):
        """
        Run demography study for all of the author activities and add the resulting enriched items.
        """
        data_source = enrich_backend.__class__.__name__.split("Enrich")[0].lower()
        log_prefix = "[{}] Demography".format(data_source)
        logger.info(
            "{} starting study {}".format(
                log_prefix, anonymize_url(self.elastic.index_url)
            )
        )

        self.run_demography(date_field, author_field, log_prefix)

        if not self.elastic.alias_in_use(alias):
            logger.info("{} Creating alias: {}".format(log_prefix, alias))
            self.elastic.add_alias(alias)

        logger.info(
            "{} end {}".format(log_prefix, anonymize_url(self.elastic.index_url))
        )

    def run_demography(
        self, date_field, author_field, log_prefix, contribution_type=None
    ):
        """
        The goal of the algorithm is to add to all enriched items the first and last date
        of all the activities or an specific contribution type of the author activities.
        """
        # The first step is to find the current min and max date for all the authors
        authors_min_max_data = self.fetch_authors_min_max_dates(
            log_prefix, author_field, contribution_type, date_field
        )

        # Then we update the min max dates of all authors
        for author in authors_min_max_data:
            author_min_date = author["min"]["value_as_string"]
            author_max_date = author["max"]["value_as_string"]
            author_key = author["key"]["author_uuid"]
            field_name = contribution_type if contribution_type else "demography"
            es_update = self.update_author_min_max_date(
                author_min_date,
                author_max_date,
                author_key,
                field_name,
                author_field=author_field,
            )

            try:
                r = self.requests.post(
                    self.elastic.index_url
                    + "/_update_by_query?wait_for_completion=true&conflicts=proceed",
                    data=es_update,
                    headers=HEADER_JSON,
                    verify=False,
                )
                self.check_version_conflicts(
                    es_update, r.json().get("version_conflicts", None), log_prefix
                )

            except requests.exceptions.RetryError:
                logger.warning(
                    "{} retry exceeded while executing demography."
                    " The following query is skipped {}".format(log_prefix, es_update)
                )
                continue

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                logger.error(
                    "{} error updating mix and max date for author {}. Aborted.".format(
                        log_prefix, author_key
                    )
                )
                logger.error(ex)
                return

    def fetch_authors_min_max_dates(
        self, log_prefix, author_field, contribution_type, date_field
    ):
        """Fetch all authors with their first and last date of activity."""
        after = None

        while True:
            es_query = self.authors_min_max_dates(
                date_field,
                author_field=author_field,
                contribution_type=contribution_type,
                after=after,
            )
            r = self.requests.post(
                self.elastic.index_url + "/_search",
                data=es_query,
                headers=HEADER_JSON,
                verify=False,
            )
            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                logger.error(
                    "{} error getting authors mix and max date. Aborted.".format(
                        log_prefix
                    )
                )
                logger.error(ex)
                return

            aggregations_author = r.json()["aggregations"]["author"]

            # When there are no more elements, it will return an empty list of buckets
            if not aggregations_author["buckets"]:
                return

            after = aggregations_author["after_key"][author_field]

            for author in aggregations_author["buckets"]:
                yield author

    def check_version_conflicts(
        self, es_update, version_conflicts, log_prefix, max_retries=5
    ):
        """
        Check if there are version conflicts within a query response and retries the request.
        """
        if version_conflicts == 0 or max_retries == 0:
            return

        logger.debug(
            "{}: Found version_conflicts: {}, retries left: {}, retry query: {}".format(
                log_prefix, version_conflicts, max_retries, es_update
            )
        )
        time.sleep(0.5)  # Wait 0.5 second between requests
        r = self.requests.post(
            self.elastic.index_url
            + "/_update_by_query?wait_for_completion=true&conflicts=proceed",
            data=es_update,
            headers=HEADER_JSON,
            verify=False,
        )
        r.raise_for_status()
        retries = max_retries - 1
        self.check_version_conflicts(
            es_update,
            r.json().get("version_conflicts", None),
            log_prefix,
            max_retries=retries,
        )

    @staticmethod
    def authors_min_max_dates(
        date_field, author_field="author_uuid", contribution_type=None, after=None
    ):
        """
        Get the aggregation of author with their min and max activity dates
        """

        # Limit aggregations:
        # - OpenSearch: 10000
        #   - https://opensearch.org/docs/latest/opensearch/bucket-agg/
        # - ElasticSearch: 10000
        #   - https://discuss.elastic.co/t/increasing-max-buckets-for-specific-visualizations/187390/4
        #   - When you try to fetch more than 10000 it will return this error message:
        #     {
        #       "type": "too_many_buckets_exception",
        #       "reason": "Trying to create too many buckets. Must be less than or equal to: [10000] but was [20000].
        #                 This limit can be set by changing the [search.max_buckets] cluster level setting.",
        #       "max_buckets": 10000
        #     }

        query_type = ""
        if contribution_type:
            query_type = (
                """"query": {
            "bool" : {
              "must" : {
                "term" : {
                  "type" : "%s"
                }
              }
            }
          },"""
                % contribution_type
            )

        query_after = ""
        if after:
            query_after = """"after": {
                  "%s": "%s"
                },""" % (
                author_field,
                after,
            )

        es_query = """
        {
          "size": 0,
          %s
          "aggs": {
            "author": {
              "composite": {
                "sources": [
                  {
                    "%s": {
                      "terms": {
                        "field": "%s"
                      }
                    }
                  }
                ],
                %s
                "size": 10000
              },
              "aggs": {
                "min": {
                  "min": {
                    "field": "%s"
                  }
                },
                "max": {
                  "max": {
                    "field": "%s"
                  }
                }
              }
            }
          }
        }
        """ % (
            query_type,
            author_field,
            author_field,
            query_after,
            date_field,
            date_field,
        )

        return es_query

    @staticmethod
    def fetch_contribution_types():
        query = """
        {
            "size":"0",
            "aggs" : {
                "uniq_gender" : {
                    "terms" : { "field" : "type" }
                }
            }
        }
        """
        return query

    @staticmethod
    def update_author_min_max_date(
        min_date, max_date, target_author, field, author_field="author_uuid"
    ):
        """
        Get the query to update demography_min_date and demography_max_date of a given author
        """

        es_query = """
        {
          "script": {
            "source":
            "ctx._source.%s_min_date = params.min_date;ctx._source.%s_max_date = params.max_date;",
            "lang": "painless",
            "params": {
                "min_date": "%s",
                "max_date": "%s"
            }
          },
          "query": {
            "term": {
              "%s": "%s"
            }
          }
        }
        """ % (
            field,
            field,
            min_date,
            max_date,
            author_field,
            target_author,
        )

        return es_query

    def enrich_feelings(
        self,
        ocean_backend,
        enrich_backend,
        attributes,
        nlp_rest_url,
        no_incremental=False,
        uuid_field="id",
        date_field="grimoire_creation_date",
    ):
        """
        This study allows to add sentiment and emotion data to a target enriched index.
        """
        es_query = (
            """
            {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "bool": {
                                    "must_not": {
                                        "exists": {
                                            "field": "has_sentiment"
                                        }
                                    }
                                }
                            },
                            {
                                "bool": {
                                    "must_not": {
                                        "exists": {
                                            "field": "has_emotion"
                                        }
                                    }
                                }
                            }
                        ]
                    }
                },
                "sort": [
                    {
                        "%s": {
                           "order": "asc"
                        }
                    }
                ]
            }
            """
            % date_field
        )

        logger.info(
            "[enrich-feelings] Start study on {} with data from {}".format(
                anonymize_url(self.elastic.index_url), nlp_rest_url
            )
        )

        es = ES(
            [self.elastic.url],
            timeout=3600,
            max_retries=50,
            retry_on_timeout=True,
            verify_certs=self.elastic.requests.verify,
            connection_class=RequestsHttpConnection,
            ssl_show_warn=self.elastic.requests.verify,
        )
        search_fields = [attr for attr in attributes]
        search_fields.extend([uuid_field])
        page = es.search(
            index=enrich_backend.elastic.index,
            scroll="1m",
            _source=search_fields,
            size=100,
            body=json.loads(es_query),
        )

        scroll_id = page["_scroll_id"]
        total = page["hits"]["total"]
        if isinstance(total, dict):
            scroll_size = total["value"]
        else:
            scroll_size = total

        if scroll_size == 0:
            logging.warning("No data found!")
            return

        total = 0
        sentiments_data = {}
        emotions_data = {}
        while scroll_size > 0:

            for hit in page["hits"]["hits"]:
                source = hit["_source"]
                source_uuid = str(source[uuid_field])
                total += 1

                for attr in attributes:
                    found = source.get(attr, None)
                    if not found:
                        continue
                    else:
                        found = found.encode("utf-8")

                    sentiment_label, emotion_label = self.get_feelings(
                        found, nlp_rest_url
                    )
                    self.__update_feelings_data(
                        sentiments_data, sentiment_label, source_uuid
                    )
                    self.__update_feelings_data(
                        emotions_data, emotion_label, source_uuid
                    )

            if sentiments_data:
                self.__add_feelings_to_index("sentiment", sentiments_data, uuid_field)
                sentiments_data = {}
            if emotions_data:
                self.__add_feelings_to_index("emotion", emotions_data, uuid_field)
                emotions_data = {}

            page = es.scroll(scroll_id=scroll_id, scroll="1m")
            scroll_id = page["_scroll_id"]
            scroll_size = len(page["hits"]["hits"])

        if sentiments_data:
            self.__add_feelings_to_index("sentiment", sentiments_data, uuid_field)
        if emotions_data:
            self.__add_feelings_to_index("emotion", emotions_data, uuid_field)

        logger.info(
            "[enrich-feelings] End study. Index {} updated with data from {}".format(
                anonymize_url(self.elastic.index_url), nlp_rest_url
            )
        )

    def get_feelings(self, text, nlp_rest_url):
        """This method wraps the calls to the NLP rest service."""
        sentiment = None
        emotion = None

        headers = {"Content-Type": "text/plain"}
        plain_text_url = nlp_rest_url + "/plainTextBugTrackerMarkdown"
        r = self.requests.post(plain_text_url, data=text, headers=headers)
        r.raise_for_status()
        plain_text_json = r.json()

        headers = {"Content-Type": "application/json"}
        code_url = nlp_rest_url + "/code"
        r = self.requests.post(
            code_url, data=json.dumps(plain_text_json), headers=headers
        )
        r.raise_for_status()
        code_json = r.json()

        texts = [c["text"] for c in code_json if c["label"] != "__label__Code"]
        message = ".".join(texts)
        message_dump = json.dumps([message])

        if not message:
            logger.debug(
                "[enrich-feelings] No feelings detected after processing on {} in index {}".format(
                    text, anonymize_url(self.elastic.index_url)
                )
            )
            return sentiment, emotion

        sentiment_url = nlp_rest_url + "/sentiment"
        headers = {"Content-Type": "application/json"}
        r = self.requests.post(sentiment_url, data=message_dump, headers=headers)
        r.raise_for_status()
        sentiment_json = r.json()[0]
        sentiment = sentiment_json["label"]

        emotion_url = nlp_rest_url + "/emotion"
        headers = {"Content-Type": "application/json"}
        r = self.requests.post(emotion_url, data=message_dump, headers=headers)
        r.raise_for_status()
        emotion_json = r.json()[0]

        emotion = (
            emotion_json["labels"][0]
            if len(emotion_json.get("labels", [])) > 0
            else None
        )
        return sentiment, emotion

    def __update_feelings_data(self, data, label, source_uuid):
        if not label:
            entry = data.get("__label__unknown", None)
            if not entry:
                data.update({"__label__unknown": [source_uuid]})
            else:
                entry.append(source_uuid)
        else:
            entry = data.get(label, None)
            if not entry:
                data.update({label: [source_uuid]})
            else:
                entry.append(source_uuid)

    def __add_feelings_to_index(self, feeling_type, feeling_data, uuid_field):
        url = "{}/_update_by_query?wait_for_completion=true".format(
            self.elastic.index_url
        )
        for fd in feeling_data:
            uuids = json.dumps(feeling_data[fd])
            es_query = """
                {
                  "script": {
                    "source": "ctx._source.feeling_%s = '%s';ctx._source.has_%s = 1",
                    "lang": "painless"
                  },
                  "query": {
                    "bool": {
                      "filter": {
                        "terms": {
                            "%s": %s
                        }
                      }
                    }
                  }
                }
                """ % (
                feeling_type,
                fd,
                feeling_type,
                uuid_field,
                uuids,
            )

            r = self.requests.post(
                url, data=es_query, headers=HEADER_JSON, verify=False
            )
            try:
                r.raise_for_status()
                logger.debug(
                    "[enrich-feelings] Adding {} on uuids {} in {}".format(
                        fd, uuids, anonymize_url(self.elastic.index_url)
                    )
                )
            except requests.exceptions.HTTPError as ex:
                logger.error(
                    "[enrich-feelings] Error adding {} on uuids {} in {}".format(
                        fd, uuids, anonymize_url(self.elastic.index_url)
                    )
                )
                logger.error(ex)

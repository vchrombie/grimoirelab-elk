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
import datetime
import json
import functools
import logging
import sys

from perceval.backend import find_signature_parameters
from grimoirelab_toolkit.datetime import datetime_utcnow, str_to_datetime

from ..elastic_analyzer import Analyzer
from ..elastic_items import ElasticItems

from .utils import grimoire_con, METADATA_FILTER_RAW, REPO_LABELS, anonymize_url
from .identities import Identities
from .studies import Studies

from .. import __version__

logger = logging.getLogger(__name__)

try:
    from sortinghat.cli.client import SortingHatClient

    SORTINGHAT_LIBS = True
except ImportError:
    logger.info("SortingHat not available")
    SORTINGHAT_LIBS = False

UNKNOWN_PROJECT = "unknown"
DEFAULT_PROJECT = "Main"
DEFAULT_DB_USER = "root"
CUSTOM_META_PREFIX = "cm"
EXTRA_PREFIX = "extra"
SH_UNKNOWN_VALUE = "Unknown"


def metadata(func):
    """Add metadata to an item.

    Decorator that adds metadata to a given item such as
    the gelk revision used.

    """

    @functools.wraps(func)
    def decorator(self, *args, **kwargs):
        eitem = func(self, *args, **kwargs)
        metadata = {
            "metadata__gelk_version": self.gelk_version,
            "metadata__gelk_backend_name": self.__class__.__name__,
            "metadata__enriched_on": datetime_utcnow().isoformat(),
        }
        eitem.update(metadata)
        return eitem

    return decorator


class Enrich(ElasticItems):
    analyzer = Analyzer
    sh_db = None
    kibiter_version = None
    roles = []
    RAW_FIELDS_COPY = [
        "metadata__updated_on",
        "metadata__timestamp",
        "offset",
        "origin",
        "tag",
        "uuid",
    ]
    KEYWORD_MAX_LENGTH = (
        1000  # this control allows to avoid max_bytes_length_exceeded_exception
    )

    ONION_INTERVAL = seconds = 3600 * 24 * 7

    def __init__(
        self,
        db_sortinghat=None,
        json_projects_map=None,
        db_user="",
        db_password="",
        db_host="",
        insecure=True,
        db_port=None,
        db_path=None,
        db_ssl=False,
        db_verify_ssl=True,
        db_tenant=None,
    ):

        perceval_backend = None
        super().__init__(perceval_backend, insecure=insecure)
        self._connector_name = None
        self.sortinghat = False
        if db_user == "":
            db_user = DEFAULT_DB_USER
        if db_sortinghat and not SORTINGHAT_LIBS:
            raise RuntimeError("Sorting hat configured but libraries not available.")
        if db_sortinghat:
            if not Enrich.sh_db:
                client = SortingHatClient(
                    host=db_host,
                    port=db_port,
                    path=db_path,
                    ssl=db_ssl,
                    verify_ssl=db_verify_ssl,
                    user=db_user,
                    password=db_password,
                    tenant=db_tenant,
                )
                client.connect()
                client.gqlc.logger.setLevel(logging.CRITICAL)
                Enrich.sh_db = client

            self.sortinghat = True

        self.prjs_map = None  # mapping beetween repositories and projects
        self.json_projects = None

        if json_projects_map:
            with open(json_projects_map) as data_file:
                self.json_projects = json.load(data_file)
                # If we have JSON projects always use them for mapping
                self.prjs_map = self.__convert_json_to_projects_map(self.json_projects)

        if self.prjs_map and self.json_projects:
            # logger.info("Comparing db and json projects")
            # self.__compare_projects_map(self.prjs_map, self.json_projects)
            pass

        self.studies = []

        self.requests = grimoire_con()
        self.elastic = None
        self.type_name = "items"  # type inside the index to store items enriched

        # To add the gelk version to enriched items
        self.gelk_version = __version__

        # params used to configure the backend
        # in perceval backends managed directly inside the backend
        self.backend_params = None
        # Label used during enrichment for identities without a known affiliation
        self.unaffiliated_group = "Unknown"
        # Label used during enrichment for identities with no gender info
        self.unknown_gender = "Unknown"

        self.identities_manager = Identities(self.get_connector_name(), Enrich.sh_db)
        self.studies_manager = Studies(self.elastic, self.requests)

    def set_elastic_url(self, url):
        """Elastic URL"""
        self.elastic_url = url

    def set_elastic(self, elastic):
        self.elastic = elastic
        self.studies_manager.elastic = elastic

    def set_params(self, params):
        from ..utils import get_connector_from_name

        self.backend_params = params
        backend_name = self.get_connector_name()
        # We can now create the perceval backend
        if not get_connector_from_name(backend_name):
            raise RuntimeError("Unknown backend {}".format(backend_name))
        connector = get_connector_from_name(backend_name)
        klass = connector[3]  # BackendCmd for the connector
        if not klass:
            # Non perceval backends can not be configured
            return

        backend_cmd = klass(*self.backend_params)
        parsed_args = vars(backend_cmd.parsed_args)
        init_args = find_signature_parameters(backend_cmd.BACKEND, parsed_args)
        backend_cmd.backend = backend_cmd.BACKEND(**init_args)
        self.perceval_backend = backend_cmd.backend

    def update_items(self, ocean_backend, enrich_backend):
        """Perform update operations over an enriched index, just after the enrichment
        It must be redefined in the enriched connectors"""

        return

    def __convert_json_to_projects_map(self, json):
        """Convert JSON format to the projects map format
        map[ds][repository] = project
        If a repository is in several projects assign to leaf
        Check that all JSON data is in the database

        :param json: data with the projects to repositories mapping
        :returns: the repositories to projects mapping per data source
        """
        ds_repo_to_prj = {}

        # Sent the unknown project to the end of the list.
        # This change is needed to avoid assigning repositories to
        # the `Main` project when they exist in the `unknown`
        # section and in other sections too.
        project_names = list(json.keys())
        if UNKNOWN_PROJECT in json:
            project_names.remove(UNKNOWN_PROJECT)
            project_names.append(UNKNOWN_PROJECT)

        for project in project_names:
            for ds in json[project]:
                if ds == "meta":
                    continue  # not a real data source
                if ds not in ds_repo_to_prj:
                    if ds not in ds_repo_to_prj:
                        ds_repo_to_prj[ds] = {}
                for repo in json[project][ds]:
                    repo, _ = self.extract_repo_tags(repo)
                    if repo in ds_repo_to_prj[ds]:
                        if project == ds_repo_to_prj[ds][repo]:
                            logger.debug(
                                "Duplicated repo: {} {} {}".format(ds, repo, project)
                            )
                        else:
                            if len(project.split(".")) > len(
                                ds_repo_to_prj[ds][repo].split(".")
                            ):
                                logger.debug(
                                    "Changed repo project because we found a leaf: {} leaf vs "
                                    "{} ({}, {})".format(
                                        project, ds_repo_to_prj[ds][repo], repo, ds
                                    )
                                )
                                ds_repo_to_prj[ds][repo] = project
                    else:
                        ds_repo_to_prj[ds][repo] = project
        return ds_repo_to_prj

    def __compare_projects_map(self, db, json):
        # Compare the projects coming from db and from a json file in eclipse
        ds_map_db = {}
        ds_map_json = {
            "git": "scm",
            "pipermail": "mls",
            "gerrit": "scr",
            "bugzilla": "its",
        }
        for ds in ds_map_json:
            ds_map_db[ds_map_json[ds]] = ds

        db_projects = []
        dss = db.keys()

        # Check that all db data is in the JSON file
        for ds in dss:
            for repository in db[ds]:
                # A repository could be in more than one project. But we get only one.
                project = db[ds][repository]
                if project not in db_projects:
                    db_projects.append(project)
                if project not in json:
                    logger.error("Project not found in JSON {}".format(project))
                    raise Exception("Project not found in JSON {}".format(project))
                else:
                    if ds == "mls":
                        repo_mls = repository.split("/")[-1]
                        repo_mls = repo_mls.replace(".mbox", "")
                        repository = (
                            "https://dev.eclipse.org/mailman/listinfo/" + repo_mls
                        )
                    if ds_map_db[ds] not in json[project]:
                        logger.error(
                            "db repository not found in json {}".format(repository)
                        )
                    elif repository not in json[project][ds_map_db[ds]]:
                        logger.error(
                            "db repository not found in json {}".format(repository)
                        )

        for project in json.keys():
            if project not in db_projects:
                logger.debug("JSON project {} not found in db".format(project))

        # Check that all JSON data is in the database
        for project in json:
            for ds in json[project]:
                if ds not in ds_map_json:
                    # meta
                    continue
                for repo in json[project][ds]:
                    if ds == "pipermail":
                        repo_mls = repo.split("/")[-1]
                        repo = "/mnt/mailman_archives/%s.mbox/%s.mbox" % (
                            repo_mls,
                            repo_mls,
                        )
                    if repo in db[ds_map_json[ds]]:
                        # print("Found ", repo, ds)
                        pass
                    else:
                        logger.debug(
                            "Not found repository in db {} {}".format(repo, ds)
                        )

        logger.debug("Number of db projects: {}".format(db_projects))
        logger.debug(
            "Number of json projects: {} (>={})".format(json.keys(), db_projects)
        )

    def get_field_unique_id(self):
        """Field in the raw item with the unique id"""
        return "uuid"

    def get_field_event_unique_id(self):
        """Field in the rich event with the unique id"""
        raise NotImplementedError

    @metadata
    def get_rich_item(self, item):
        """Create a rich item from the raw item"""
        raise NotImplementedError

    def get_rich_events(self, item):
        """Create rich events from the raw item"""
        raise NotImplementedError

    def enrich_events(self, items):
        return self.enrich_items(items, events=True)

    def enrich_items(self, ocean_backend, events=False):
        """
        Enrich the items fetched from ocean_backend generator
        generating enriched items/events which are uploaded to the Elasticsearch index for
        this Enricher (self).

        :param ocean_backend: Ocean backend object to fetch the items from
        :param events: enrich items or enrich events
        :return: total number of enriched items/events uploaded to Elasticsearch
        """

        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        items = ocean_backend.fetch()

        url = self.elastic.get_bulk_url()

        logger.debug(
            "Adding items to {} (in {} packs)".format(anonymize_url(url), max_items)
        )

        if events:
            logger.debug("Adding events items")

        for item in items:
            if current >= max_items:
                try:
                    total += self.elastic.safe_put_bulk(url, bulk_json)
                    json_size = sys.getsizeof(bulk_json) / (1024 * 1024)
                    logger.debug(
                        "Added {} items to {} ({:.2f} MB)".format(
                            total, anonymize_url(url), json_size
                        )
                    )
                except UnicodeEncodeError:
                    # Why is requests encoding the POST data as ascii?
                    logger.error("Unicode error in enriched items")
                    logger.debug(bulk_json)
                    safe_json = str(bulk_json.encode("ascii", "ignore"), "ascii")
                    total += self.elastic.safe_put_bulk(url, safe_json)
                bulk_json = ""
                current = 0

            if not events:
                rich_item = self.get_rich_item(item)
                data_json = json.dumps(rich_item)
                bulk_json += '{"index" : {"_id" : "%s" } }\n' % (
                    item[self.get_field_unique_id()]
                )
                bulk_json += data_json + "\n"  # Bulk document
                current += 1
            else:
                rich_events = self.get_rich_events(item)
                for rich_event in rich_events:
                    data_json = json.dumps(rich_event)
                    bulk_json += '{"index" : {"_id" : "%s_%s" } }\n' % (
                        item[self.get_field_unique_id()],
                        rich_event[self.get_field_event_unique_id()],
                    )
                    bulk_json += data_json + "\n"  # Bulk document
                    current += 1

        if current > 0:
            total += self.elastic.safe_put_bulk(url, bulk_json)

        return total

    def add_repository_labels(self, eitem):
        """Add labels to the enriched item"""

        eitem[REPO_LABELS] = self.repo_labels

    def add_metadata_filter_raw(self, eitem):
        """Add filter raw information to the enriched item"""

        eitem[METADATA_FILTER_RAW] = self.filter_raw

    def get_connector_name(self):
        """Find the name for the current connector"""
        from ..utils import get_connector_name

        if not self._connector_name:
            self._connector_name = get_connector_name(type(self))
        return self._connector_name

    def get_sh_backend_name(self):
        """Retrieve the backend name for SortingHat identities."""

        return self.get_connector_name()

    def get_field_author(self):
        """Field with the author information"""
        raise NotImplementedError

    def get_field_date(self):
        """Field with the date in the JSON enriched items"""
        return "metadata__updated_on"

    def get_identities(self, item):
        """Return the identities from an item"""
        raise NotImplementedError

    def has_identities(self):
        """Return whether the enriched items contains identities"""

        return True

    def get_email_domain(self, email):
        domain = None
        try:
            domain = email.split("@")[1]
        except (IndexError, AttributeError):
            # logger.warning("Bad email format: %s" % (identity['email']))
            pass
        return domain

    def get_identity_domain(self, identity):
        domain = None
        if "email" in identity and identity["email"]:
            domain = self.get_email_domain(identity["email"])
        return domain

    def get_item_id(self, eitem):
        """Return the item_id linked to this enriched eitem"""

        # If possible, enriched_item and item will have the same id
        return eitem["_id"]

    def get_last_update_from_es(self, _filters=[]):

        last_update = self.elastic.get_last_date(self.get_incremental_date(), _filters)

        return last_update

    def get_last_offset_from_es(self, _filters=[]):
        # offset is always the field name from perceval
        last_update = self.elastic.get_last_offset("offset", _filters)

        return last_update

    # def get_elastic_mappings(self):
    #     """ Mappings for enriched indexes """
    #
    #     mapping = '{}'
    #     return {"items": mapping}

    def get_elastic_analyzers(self):
        """Custom analyzers for our indexes"""

        analyzers = "{}"

        return analyzers

    def get_grimoire_fields(self, creation_date, item_name):
        """Return common grimoire fields for all data sources"""

        grimoire_date = None
        if isinstance(creation_date, datetime.datetime):
            grimoire_date = creation_date.isoformat()
        else:
            try:
                grimoire_date = str_to_datetime(creation_date).isoformat()
            except Exception as ex:
                pass

        name = "is_" + self.get_connector_name() + "_" + item_name

        return {"grimoire_creation_date": grimoire_date, name: 1}

    # Project field enrichment
    def get_project_repository(self, eitem):
        """
        Get the repository name used for mapping to project name from
        the enriched item.
        To be implemented for each data source
        """
        return ""

    @classmethod
    def add_project_levels(cls, project):
        """Add project sub levels extra items"""

        eitem_path = ""
        eitem_project_levels = {}

        if project is not None:
            subprojects = project.split(".")
            for i in range(0, len(subprojects)):
                if i > 0:
                    eitem_path += "."
                eitem_path += subprojects[i]
                eitem_project_levels["project_" + str(i + 1)] = eitem_path

        return eitem_project_levels

    def find_item_project(self, eitem):
        """
        Find the project for a enriched item
        :param eitem: enriched item for which to find the project
        :return: the project entry (a dictionary)
        """
        # get the data source name relying on the cfg section name, if null use the connector name
        ds_name = (
            self.cfg_section_name
            if self.cfg_section_name
            else self.get_connector_name()
        )

        try:
            # retrieve the project which includes the repo url in the projects.json,
            # the variable `projects_json_repo` is passed from mordred to ELK when
            # iterating over the repos in the projects.json, (see: param
            # `projects_json_repo` in the functions elk.feed_backend and
            # elk.enrich_backend)
            if self.projects_json_repo:
                project = self.prjs_map[ds_name][self.projects_json_repo]
            # if `projects_json_repo` (e.g., AOC study), use the
            # method `get_project_repository` (defined in each enricher)
            else:
                repository = self.get_project_repository(eitem)
                project = self.prjs_map[ds_name][repository]
        # With the introduction of `projects_json_repo` the code in the
        # except should be unreachable, and could be removed
        except KeyError:
            # logger.warning("Project not found for repository %s (data source: %s)", repository, ds_name)
            project = None

            if self.filter_raw:
                fltr = eitem["origin"] + " --filter-raw=" + self.filter_raw
                if ds_name in self.prjs_map and fltr in self.prjs_map[ds_name]:
                    project = self.prjs_map[ds_name][fltr]
            elif ds_name in self.prjs_map:
                # this code is executed to retrieve the project of private repositories (in particular Git ones)
                # the URLs in the prjs_map are retrieved, anonymized and compared with the value
                # returned by `get_project_repository`
                repository = self.get_project_repository(eitem)
                for r in self.prjs_map[ds_name]:
                    anonymized_repo = anonymize_url(r)
                    if repository == anonymized_repo:
                        project = self.prjs_map[ds_name][r]
                        break

            if project == UNKNOWN_PROJECT:
                return None
            if project:
                return project

            # Try to use always the origin in any case
            if "origin" in eitem:
                if (
                    ds_name in self.prjs_map
                    and eitem["origin"] in self.prjs_map[ds_name]
                ):
                    project = self.prjs_map[ds_name][eitem["origin"]]
                elif ds_name in self.prjs_map:
                    # Try to find origin as part of the keys
                    for ds_repo in self.prjs_map[ds_name]:
                        ds_repo = str(ds_repo)  # discourse has category_id ints
                        if eitem["origin"] in ds_repo:
                            project = self.prjs_map[ds_name][ds_repo]
                            break

        if project == UNKNOWN_PROJECT:
            project = None

        return project

    def get_item_project(self, eitem):
        """
        Get the project name related to the eitem
        :param eitem: enriched item for which to find the project
        :return: a dictionary with the project data
        """
        eitem_project = {}
        project = self.find_item_project(eitem)

        if project is None:
            project = DEFAULT_PROJECT

        eitem_project = {"project": project}
        # Time to add the project levels: eclipse.platform.releng.aggregator
        eitem_project.update(self.add_project_levels(project))

        # And now time to add the metadata
        eitem_project.update(self.get_item_metadata(eitem))

        return eitem_project

    def get_item_metadata(self, eitem):
        """
        In the projects.json file, inside each project, there is a field called "meta" which has a
        dictionary with fields to be added to the enriched items for this project.

        This fields must be added with the prefix cm_ (custom metadata).

        This method fetch the metadata fields for the project in which the eitem is included.

        :param eitem: enriched item to search metadata for
        :return: a dictionary with the metadata fields
        """

        eitem_metadata = {}

        # Get the project entry for the item, which includes the metadata
        project = self.find_item_project(eitem)

        if project and "meta" in self.json_projects[project]:
            meta_fields = self.json_projects[project]["meta"]
            if isinstance(meta_fields, dict):
                eitem_metadata = {
                    CUSTOM_META_PREFIX + "_" + field: value
                    for field, value in meta_fields.items()
                }

        return eitem_metadata

    # Sorting Hat stuff to be moved to SortingHat class
    def get_sh_identity(self, item, identity_field):
        """Empty identity. Real implementation in each data source."""
        identity = {}
        for field in ["name", "email", "username"]:
            identity[field] = None
        return identity

    @staticmethod
    def get_main_enrollments(enrollments):
        return Identities.get_main_enrollments(enrollments)

    @staticmethod
    def remove_prefix_enrollments(enrollments):
        return Identities.remove_prefix_enrollments(enrollments)

    def get_item_no_sh_fields(self, identity, rol):
        return self.identities_manager.get_item_no_sh_fields(
            identity, rol, self.generate_uuid, self.get_identity_domain
        )

    def get_individual_fields(
        self, individual, sh_id=None, item_date=None, rol="author"
    ):
        return self.identities_manager.get_individual_fields(
            individual, self.get_email_domain, sh_id, item_date, rol
        )

    def get_item_sh_fields(
        self, identity=None, item_date=None, sh_id=None, rol="author"
    ):
        return self.identities_manager.get_item_sh_fields(
            self.get_identity_domain,
            self.get_email_domain,
            identity,
            item_date,
            sh_id,
            rol,
        )

    def get_sh_item_from_id(self, sh_id):
        return self.identities_manager.get_sh_item_from_id(sh_id)

    def get_sh_item_from_identity(self, identity, backend_name):
        return self.identities_manager.get_sh_item_from_identity(identity, backend_name)

    def get_sh_item_from_identity_cache(self, identity_tuple, backend_name):
        return self.identities_manager.get_sh_item_from_identity_cache(
            identity_tuple, backend_name
        )

    def get_sh_item_multi_enrollments(self, enrollments, item_date_str):
        return self.identities_manager.get_sh_item_multi_enrollments(
            enrollments, item_date_str
        )

    def get_item_sh_from_id(self, eitem, roles=None, individuals=None):
        return self.identities_manager.get_item_sh_from_id(
            eitem, self.get_field_author, self.get_field_date, roles, individuals
        )

    def get_item_sh_meta_fields(
        self,
        eitem,
        roles=None,
        suffixes=None,
        non_authored_prefix=None,
        individuals=None,
    ):
        return self.identities_manager.get_item_sh_meta_fields(
            eitem,
            self.get_field_date,
            roles,
            suffixes,
            non_authored_prefix,
            individuals,
        )

    @staticmethod
    def find_individual(individuals, sh_id):
        return Identities.find_individual(individuals, sh_id)

    def add_meta_fields(
        self, eitem, meta_eitem, sh_fields, rol, uuid, suffixes, non_authored_prefix
    ):
        return self.identities_manager.add_meta_fields(
            eitem, meta_eitem, sh_fields, rol, uuid, suffixes, non_authored_prefix
        )

    def get_users_data(self, item):
        return self.identities_manager.get_users_data(item)

    def get_item_sh(self, item, roles=None, date_field=None):
        return self.identities_manager.get_item_sh(
            item,
            self.get_field_author,
            self.get_field_date,
            self.get_sh_identity,
            self.get_identity_domain,
            roles,
            date_field,
        )

    def generate_uuid(self, source, email=None, name=None, username=None):
        return self.identities_manager.generate_uuid(source, email, name, username)

    def get_entity(self, id):
        return self.identities_manager.get_entity(id)

    def is_bot(self, uuid):
        return self.identities_manager.is_bot(uuid)

    def get_enrollments(self, uuid):
        return self.identities_manager.get_enrollments(uuid)

    def get_unique_identity(self, uuid):
        return self.identities_manager.get_unique_identity(uuid)

    def get_uuid_from_id(self, sh_id):
        return self.identities_manager.get_uuid_from_id(sh_id)

    def add_sh_identities(self, identities):
        self.identities_manager.add_sh_identities(identities)

    def add_sh_identity_cache(self, identity_tuple):
        self.identities_manager.add_sh_identity_cache(identity_tuple)

    def add_sh_identity(self, identity):
        self.identities_manager.add_sh_identity(identity)

    def copy_raw_fields(self, copy_fields, source, target):
        """Copy fields from item to enriched item."""

        for f in copy_fields:
            if f in source:
                target[f] = source[f]
            else:
                target[f] = None

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
        seconds=ONION_INTERVAL,
        no_incremental=False,
    ):
        return self.studies_manager.enrich_onion(
            enrich_backend,
            alias,
            in_index,
            out_index,
            data_source,
            contribs_field,
            timeframe_field,
            sort_on_field,
            seconds,
            no_incremental,
        )

    def enrich_extra_data(
        self, ocean_backend, enrich_backend, json_url, target_index=None
    ):
        return self.studies_manager.enrich_extra_data(
            ocean_backend, enrich_backend, json_url, target_index
        )

    def find_geo_point_in_index(
        self, es_in, in_index, location_field, location_value, geolocation_field
    ):
        return self.studies_manager.find_geo_point_in_index(
            es_in, in_index, location_field, location_value, geolocation_field
        )

    def add_geo_point_in_index(
        self,
        enrich_backend,
        geolocation_field,
        loc_lat,
        loc_lon,
        location_field,
        location,
    ):
        return self.studies_manager.add_geo_point_in_index(
            enrich_backend,
            geolocation_field,
            loc_lat,
            loc_lon,
            location_field,
            location,
        )

    def enrich_geolocation(
        self, ocean_backend, enrich_backend, location_field, geolocation_field
    ):
        return self.studies_manager.enrich_geolocation(
            ocean_backend, enrich_backend, location_field, geolocation_field
        )

    def enrich_forecast_activity(
        self,
        ocean_backend,
        enrich_backend,
        out_index,
        observations=20,
        probabilities=[0.5, 0.7, 0.9],
        interval_months=6,
        date_field="metadata__updated_on",
    ):
        return self.studies_manager.enrich_forecast_activity(
            ocean_backend,
            enrich_backend,
            out_index,
            self.gelk_version,
            self.__class__.__name__,
            self.get_field_unique_id,
            self.get_grimoire_fields,
            observations,
            probabilities,
            interval_months,
            date_field,
        )

    def dates_to_duration(self, dates, *, window_size=20):
        return self.studies_manager.dates_to_duration(dates, window_size=window_size)

    @staticmethod
    def authors_between_dates(
        repository_url,
        min_date,
        max_date,
        author_field="author_uuid",
        date_field="metadata__updated_on",
    ):
        return Studies.authors_between_dates(
            repository_url, min_date, max_date, author_field, date_field
        )

    @staticmethod
    def author_activity(
        repository_url,
        min_date,
        max_date,
        author_value,
        author_field="author_uuid",
        date_field="metadata__updated_on",
    ):
        return Studies.author_activity(
            repository_url, min_date, max_date, author_value, author_field, date_field
        )

    def enrich_demography_contribution(
        self,
        ocean_backend,
        enrich_backend,
        alias,
        date_field="grimoire_creation_date",
        author_field="author_uuid",
    ):
        return self.studies_manager.enrich_demography_contribution(
            ocean_backend, enrich_backend, alias, date_field, author_field
        )

    def enrich_demography(
        self,
        ocean_backend,
        enrich_backend,
        alias,
        date_field="grimoire_creation_date",
        author_field="author_uuid",
    ):
        return self.studies_manager.enrich_demography(
            ocean_backend, enrich_backend, alias, date_field, author_field
        )

    def run_demography(
        self, date_field, author_field, log_prefix, contribution_type=None
    ):
        return self.studies_manager.run_demography(
            date_field, author_field, log_prefix, contribution_type
        )

    def fetch_authors_min_max_dates(
        self, log_prefix, author_field, contribution_type, date_field
    ):
        return self.studies_manager.fetch_authors_min_max_dates(
            log_prefix, author_field, contribution_type, date_field
        )

    def check_version_conflicts(
        self, es_update, version_conflicts, log_prefix, max_retries=5
    ):
        self.studies_manager.check_version_conflicts(
            es_update, version_conflicts, log_prefix, max_retries
        )

    @staticmethod
    def authors_min_max_dates(
        date_field, author_field="author_uuid", contribution_type=None, after=None
    ):
        return Studies.authors_min_max_dates(
            date_field, author_field, contribution_type, after
        )

    @staticmethod
    def fetch_contribution_types():
        return Studies.fetch_contribution_types()

    @staticmethod
    def update_author_min_max_date(
        min_date, max_date, target_author, field, author_field="author_uuid"
    ):
        return Studies.update_author_min_max_date(
            min_date, max_date, target_author, field, author_field
        )

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
        return self.studies_manager.enrich_feelings(
            ocean_backend,
            enrich_backend,
            attributes,
            nlp_rest_url,
            no_incremental,
            uuid_field,
            date_field,
        )

    def get_feelings(self, text, nlp_rest_url):
        return self.studies_manager.get_feelings(text, nlp_rest_url)
